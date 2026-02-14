"""
Loan API Lambda — Frontend API Layer
=====================================

Single Lambda function behind API Gateway HttpApi with path-based routing.

Endpoints:
  POST /apply           — Submit a loan application, invoke demo workflow async
  GET  /status/{id}     — Poll for workflow progress and logs
  POST /approve/{id}    — Manager approval callback (resume suspended workflow)

Environment variables:
  PROGRESS_TABLE        — DynamoDB table name for progress tracking
  LOAN_FUNCTION_NAME    — ARN of the loan durable Lambda (alias)
"""

import json
import os
import random
import string
import time
from datetime import datetime, timezone
from decimal import Decimal
from functools import partial

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import APIGatewayHttpResolver
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    NotFoundError,
)
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit


logger = Logger()
tracer = Tracer()
metrics = Metrics()


class DecimalEncoder(json.JSONEncoder):
    """Encode Decimal values as float for JSON serialization."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


app = APIGatewayHttpResolver(serializer=partial(json.dumps, cls=DecimalEncoder))

dynamodb = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

PROGRESS_TABLE = os.environ["PROGRESS_TABLE"]
LOAN_FUNCTION_NAME = os.environ["LOAN_FUNCTION_NAME"]


@app.exception_handler(json.JSONDecodeError)
def handle_json_decode_error(exc):
    logger.warning("Malformed JSON in request body", error=str(exc))
    raise BadRequestError("Invalid JSON body")


@app.post("/apply")
@tracer.capture_method
def apply():
    """POST /apply — Submit a new loan application."""
    body = app.current_event.json_body

    name = body.get("name", "").strip()
    address = body.get("address", "").strip()
    phone = body.get("phone", "").strip()
    sin = body.get("sin", "").strip()
    loan_amount = body.get("loan_amount")

    if not all([name, sin, loan_amount]):
        raise BadRequestError("Missing required fields: name, sin, loan_amount")

    try:
        loan_amount = float(loan_amount)
    except (ValueError, TypeError):
        raise BadRequestError("loan_amount must be a number")

    # Generate application ID
    random_suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    application_id = f"LOAN-{int(time.time())}-{random_suffix}"

    timestamp = datetime.now(timezone.utc).isoformat()

    # Create initial DynamoDB record
    table = dynamodb.Table(PROGRESS_TABLE)
    table.put_item(Item={
        "application_id": application_id,
        "status": "submitted",
        "current_step": "submitted",
        "applicant_name": name,
        "loan_amount": Decimal(str(loan_amount)),
        "logs": [{
            "timestamp": timestamp,
            "step": "submitted",
            "message": "Application received",
            "level": "info",
        }],
        "result": None,
        "created_at": timestamp,
    })

    # Build workflow event payload
    workflow_event = {
        "application_id": application_id,
        "applicant_name": name,
        "ssn_last4": sin,
        "annual_income": 85000,
        "loan_amount": loan_amount,
        "loan_purpose": "personal_loan",
        "address": address,
        "phone": phone,
    }

    # Invoke loan workflow Lambda asynchronously
    lambda_client.invoke(
        FunctionName=LOAN_FUNCTION_NAME,
        InvocationType="Event",
        Payload=json.dumps(workflow_event),
    )

    logger.info("Application created and workflow invoked", application_id=application_id)
    metrics.add_metric(name="ApplicationsSubmitted", unit=MetricUnit.Count, value=1)

    return {"application_id": application_id}


@app.get("/status/<applicationId>")
@tracer.capture_method
def status(applicationId: str):
    """GET /status/{applicationId} — Return current progress."""
    if not applicationId:
        raise BadRequestError("Missing applicationId")

    table = dynamodb.Table(PROGRESS_TABLE)
    result = table.get_item(Key={"application_id": applicationId})

    item = result.get("Item")
    if not item:
        raise NotFoundError("Application not found")

    return item


@app.post("/approve/<applicationId>")
@tracer.capture_method
def approve(applicationId: str):
    """POST /approve/{applicationId} — Manager approval sends callback to resume workflow."""
    if not applicationId:
        raise BadRequestError("Missing applicationId")

    body = app.current_event.json_body
    approved = body.get("approved", False)

    # Read callback_id from DynamoDB (stored by the workflow's setup step)
    table = dynamodb.Table(PROGRESS_TABLE)
    result = table.get_item(Key={"application_id": applicationId})
    item = result.get("Item")

    if not item:
        raise NotFoundError("Application not found")

    callback_id = item.get("callback_id")
    if not callback_id:
        raise BadRequestError("No pending approval for this application")

    # Send callback to resume the suspended durable execution
    callback_result = {
        "approved": approved,
    }
    if not approved:
        callback_result["reason"] = body.get("reason", "Manager denied the application")

    lambda_client.send_durable_execution_callback_success(
        CallbackId=callback_id,
        Result=json.dumps(callback_result),
    )

    # Clear the callback_id from DynamoDB
    table.update_item(
        Key={"application_id": applicationId},
        UpdateExpression="REMOVE callback_id",
    )

    logger.info("Approval sent", application_id=applicationId, approved=approved)
    metrics.add_metric(name="ApprovalsProcessed", unit=MetricUnit.Count, value=1)

    return {"status": "approval_sent", "approved": approved}


@tracer.capture_lambda_handler
@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_HTTP, log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event, context):
    return app.resolve(event, context)
