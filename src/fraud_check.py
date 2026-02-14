"""
External Fraud Check Service — Callback Simulator
==================================================

Simulates an external fraud-check API. The demo durable workflow
invokes this Lambda asynchronously, passing a callback_id.
After a short delay (simulating processing), this function calls
SendDurableExecutionCallbackSuccess to resume the suspended workflow.

This demonstrates the callback pattern: the durable execution suspends
with zero compute cost while an external system does work, then
resumes when the callback arrives.
"""

import json
import os
import time

import boto3
from aws_lambda_powertools import Logger, Tracer


logger = Logger()
tracer = Tracer()

lambda_client = boto3.client("lambda")

WORKFLOW_FUNCTION_NAME = os.environ["WORKFLOW_FUNCTION_NAME"]


@tracer.capture_lambda_handler
@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context):
    """Simulate fraud check processing, then send callback to resume workflow."""
    callback_id = event["callback_id"]
    applicant_name = event.get("applicant_name", "Unknown")
    application_id = event.get("application_id", "Unknown")

    logger.info(
        "Fraud check started",
        applicant_name=applicant_name,
        application_id=application_id,
        callback_id=callback_id,
    )

    # Simulate external processing time
    time.sleep(5)

    result = {
        "fraud_check": "passed",
        "risk_indicators": 0,
        "checked_by": "FraudCheckService-v2",
    }

    logger.info(
        "Fraud check passed — sending callback to resume workflow",
        application_id=application_id,
        result=result,
    )

    # Resume the suspended durable execution
    lambda_client.send_durable_execution_callback_success(
        CallbackId=callback_id,
        Result=json.dumps(result),
    )

    return {
        "status": "callback_sent",
        "callback_id": callback_id,
        "application_id": application_id,
    }
