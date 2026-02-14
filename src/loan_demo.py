"""
Loan Durable Workflow — Frontend Presentation
==============================================

Durable loan approval workflow designed for live demos with a React frontend.

Features:
  - Writes progress to DynamoDB at each step (frontend polls for updates)
  - Hardcoded scenarios based on SIN last 4 digits for predictable outcomes
  - Deliberate time.sleep() in each step to visualize progress
  - External fraud check via callback (separate Lambda sends callback to resume)
  - Manager approval callback for loans >= $100K

Hardcoded scenarios:
  - SIN ending 1111 (Alice): Always approved
  - SIN ending 2222 (Bob):   Always denied (credit score too low)
  - SIN ending 3333 (Charlie): Approved if loan_amount <= $25,000
"""

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from aws_lambda_powertools import Logger

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import (
    Duration,
    WaitForCallbackConfig,
)


logger = Logger()


# ─────────────────────────────────────────────────
# DynamoDB Progress Logging
# ─────────────────────────────────────────────────

def get_progress_table():
    dynamodb = boto3.resource("dynamodb")
    return dynamodb.Table(os.environ["PROGRESS_TABLE"])


def log_progress(table, application_id, step, message, status, level="info", result=None):
    """Append a log entry and update status in DynamoDB."""
    timestamp = datetime.now(timezone.utc).isoformat()
    log_entry = {
        "timestamp": timestamp,
        "step": step,
        "message": message,
        "level": level,
    }

    update_expr = (
        "SET #logs = list_append(if_not_exists(#logs, :empty_list), :new_log), "
        "current_step = :step, #status = :status, updated_at = :ts"
    )
    expr_values = {
        ":new_log": [log_entry],
        ":empty_list": [],
        ":step": step,
        ":status": status,
        ":ts": timestamp,
    }
    expr_names = {
        "#logs": "logs",
        "#status": "status",
    }

    if result is not None:
        update_expr += ", #result = :result"
        expr_values[":result"] = _convert_floats(result)
        expr_names["#result"] = "result"

    table.update_item(
        Key={"application_id": application_id},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names,
    )


def _convert_floats(obj):
    """Recursively convert float values to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _convert_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_floats(i) for i in obj]
    return obj


# ─────────────────────────────────────────────────
# Hardcoded Scenario Logic
# ─────────────────────────────────────────────────

def get_scenario_decision(ssn_last4, loan_amount):
    """Return the predetermined decision based on SIN last 4 digits."""
    if ssn_last4 == "1111":
        return "approved"
    elif ssn_last4 == "2222":
        return "denied"
    elif ssn_last4 == "3333":
        return "approved" if loan_amount <= 25000 else "denied"
    # Default: approve for unknown SINs
    return "approved"


# ─────────────────────────────────────────────────
# Durable Steps
# ─────────────────────────────────────────────────

@durable_step
def validate_application(step_context: StepContext, application: dict) -> dict:
    """Validate the loan application fields."""
    logger.info(
        "Validating application",
        extra={"application_id": application.get("application_id")},
    )
    time.sleep(2)

    required = [
        "application_id", "applicant_name", "ssn_last4",
        "annual_income", "loan_amount", "loan_purpose",
    ]
    missing = [f for f in required if f not in application]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    income = application["annual_income"]
    loan_amount = application["loan_amount"]

    if loan_amount <= 0:
        raise ValueError("Loan amount must be positive")
    if income <= 0:
        raise ValueError("Annual income must be positive")

    dti_estimate = (loan_amount * 0.05) / (income / 12)

    return {
        "application_id": application["application_id"],
        "applicant_name": application["applicant_name"],
        "ssn_last4": application["ssn_last4"],
        "annual_income": income,
        "loan_amount": loan_amount,
        "loan_purpose": application["loan_purpose"],
        "estimated_dti": round(dti_estimate, 2),
        "status": "validated",
        "validated_at": datetime.now(timezone.utc).isoformat(),
    }


@durable_step
def pull_credit_report(step_context: StepContext, bureau: str, ssn_last4: str) -> dict:
    """Pull a credit report from one bureau."""
    logger.info(f"Pulling credit report from {bureau}")
    time.sleep(3)

    import random
    seed = int(hashlib.md5(f"{bureau}-{ssn_last4}".encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)
    score = rng.randint(580, 820)

    return {
        "bureau": bureau,
        "score": score,
        "report_id": f"{bureau[:3].upper()}-{ssn_last4}-{score}",
        "derogatory_marks": rng.randint(0, 3),
        "open_accounts": rng.randint(2, 15),
        "pulled_at": datetime.now(timezone.utc).isoformat(),
    }


@durable_step
def calculate_risk_score(step_context: StepContext, credit_reports: list, ssn_last4: str, loan_amount: float) -> dict:
    """Aggregate credit reports and apply hardcoded scenario override."""
    logger.info("Calculating risk score from credit reports")
    time.sleep(2)

    scores = [r["score"] for r in credit_reports]
    avg_score = sum(scores) / len(scores)
    total_derogatory = sum(r["derogatory_marks"] for r in credit_reports)

    if avg_score >= 740 and total_derogatory == 0:
        tier, base_rate = "prime", 5.25
    elif avg_score >= 670:
        tier, base_rate = "near-prime", 7.50
    elif avg_score >= 580:
        tier, base_rate = "subprime", 11.00
    else:
        tier, base_rate = "deep-subprime", 15.00

    # Apply hardcoded scenario override
    decision = get_scenario_decision(ssn_last4, loan_amount)

    return {
        "average_score": round(avg_score, 1),
        "min_score": min(scores),
        "max_score": max(scores),
        "total_derogatory_marks": total_derogatory,
        "risk_tier": tier,
        "base_rate": base_rate,
        "decision": decision,
    }


@durable_step
def generate_loan_offer(step_context: StepContext, app: dict, risk: dict) -> dict:
    """Generate the final loan offer with rate and payment terms."""
    logger.info(f"Generating offer for {app['application_id']}")
    time.sleep(2)

    rate = risk["base_rate"]
    loan_amount = app["loan_amount"]
    term_months = 60
    monthly_rate = rate / 100 / 12

    if monthly_rate > 0:
        payment = loan_amount * (
            monthly_rate * (1 + monthly_rate) ** term_months
        ) / ((1 + monthly_rate) ** term_months - 1)
    else:
        payment = loan_amount / term_months

    offer_id = hashlib.sha256(
        f"offer-{app['application_id']}-{rate}".encode()
    ).hexdigest()[:10]

    return {
        "offer_id": f"OFFER-{offer_id.upper()}",
        "application_id": app["application_id"],
        "loan_amount": loan_amount,
        "annual_rate": rate,
        "term_months": term_months,
        "monthly_payment": round(payment, 2),
        "total_interest": round(payment * term_months - loan_amount, 2),
        "status": "offer_generated",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@durable_step
def request_manager_approval(step_context: StepContext, callback_id: str, application_id: str, loan_amount: float) -> dict:
    """Store callback_id in DynamoDB so the frontend can send the approval callback."""
    logger.info(
        f"Manager approval required for ${loan_amount:,.0f} loan "
        f"— callback_id: {callback_id}"
    )

    # Write callback_id to DynamoDB so the API can read it when manager approves
    table = get_progress_table()
    table.update_item(
        Key={"application_id": application_id},
        UpdateExpression="SET callback_id = :cid",
        ExpressionAttributeValues={":cid": callback_id},
    )

    return {
        "application_id": application_id,
        "callback_id": callback_id,
        "status": "pending_manager_approval",
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }


@durable_step
def request_fraud_check(step_context: StepContext, callback_id: str, application_id: str, applicant_name: str) -> dict:
    """Invoke the external fraud check Lambda, passing the callback_id so it can resume us."""
    logger.info(
        f"Requesting external fraud check for {applicant_name} "
        f"— callback_id: {callback_id}"
    )

    lambda_client = boto3.client("lambda")
    fraud_check_function = os.environ["FRAUD_CHECK_FUNCTION"]

    import json
    lambda_client.invoke(
        FunctionName=fraud_check_function,
        InvocationType="Event",
        Payload=json.dumps({
            "callback_id": callback_id,
            "application_id": application_id,
            "applicant_name": applicant_name,
        }),
    )

    return {
        "application_id": application_id,
        "callback_id": callback_id,
        "status": "fraud_check_requested",
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }


@durable_step
def disburse_funds(step_context: StepContext, offer: dict) -> dict:
    """Finalize and disburse the loan — no random failures in demo mode."""
    logger.info(
        f"Disbursing ${offer['loan_amount']:,.0f} for {offer['offer_id']}"
    )
    time.sleep(2)

    return {
        "offer_id": offer["offer_id"],
        "disbursement_ref": f"DSB-{offer['offer_id'][-6:]}",
        "amount_disbursed": offer["loan_amount"],
        "status": "funded",
        "funded_at": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────
# Main Durable Execution Handler
# ─────────────────────────────────────────────────

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    """
    Loan approval workflow with DynamoDB progress logging.
    Invoked asynchronously by the API Lambda.
    """
    table = get_progress_table()
    application_id = event["application_id"]
    logger.append_keys(application_id=application_id)

    # Detect replay: count how many log entries exist per step in DynamoDB.
    # On each invocation, track how many times we've logged each step.
    # If we've already logged that many entries for a step, it's a replay.
    from collections import Counter
    existing = table.get_item(Key={"application_id": application_id}).get("Item", {})
    prior_counts = Counter(entry["step"] for entry in existing.get("logs", []))
    call_counts = Counter()

    def log(step, message, status, level="info", result=None):
        call_counts[step] += 1
        if call_counts[step] <= prior_counts.get(step, 0):
            message = f"[REPLAY] {message}"
            level = "replay"
        log_progress(table, application_id, step, message, status, level, result)

    try:
        # ── Step 1: Validate Application ────────────────────
        log(  "validating", "Validating loan application...", "processing")
        validated = context.step(validate_application(event))

        logger.info(
            f"Application validated: {validated['application_id']} "
            f"— ${validated['loan_amount']:,.0f} ({validated['loan_purpose']})"
        )
        log("validating", "Application validated successfully", "processing")

        # ── Step 2: Parallel Credit Bureau Checks ───────────
        log("credit_check", "Pulling credit reports from 3 bureaus...", "processing")
        bureaus = ["equifax", "transunion", "experian"]

        credit_results = context.parallel([
            lambda ctx, b=b: ctx.step(
                pull_credit_report(b, validated["ssn_last4"])
            )
            for b in bureaus
        ])

        credit_reports = credit_results.get_results()
        scores_str = ", ".join(f"{r['bureau']}={r['score']}" for r in credit_reports)
        logger.info(f"Credit reports pulled — scores: {scores_str}")
        log("credit_check", f"Credit scores received: {scores_str}", "processing")

        # ── Step 3: Risk Assessment ─────────────────────────
        log("risk_assessment", "Calculating risk score...", "processing")
        risk = context.step(calculate_risk_score(
            credit_reports, validated["ssn_last4"], validated["loan_amount"]
        ))

        logger.info(
            f"Risk assessed: tier={risk['risk_tier']}, "
            f"avg={risk['average_score']}, decision={risk['decision']}"
        )
        log(
            "risk_assessment",
            f"Risk tier: {risk['risk_tier']}, avg score: {risk['average_score']}, decision: {risk['decision']}",
            "processing",
        )

        # ── Denied Path ─────────────────────────────────────
        if risk["decision"] == "denied":
            final_result = {
                "application_id": validated["application_id"],
                "applicant_name": validated["applicant_name"],
                "status": "denied",
                "reason": f"Application denied — risk tier: {risk['risk_tier']}, avg credit score: {risk['average_score']}",
                "risk_tier": risk["risk_tier"],
                "average_score": risk["average_score"],
            }
            log(
                "risk_assessment", "Application denied",
                "denied", level="warn", result=final_result,
            )
            return final_result

        # ── Step 4: Manager Approval (if >= $100,000) ────────
        if validated["loan_amount"] >= 100000:
            log(
                "manager_approval",
                f"Manager approval required for loans >= $100,000 (requested: ${validated['loan_amount']:,.0f})",
                "pending_approval",
            )

            def submit_manager_approval(callback_id, _ctx):
                """Store callback_id in DynamoDB so the frontend can send the approval."""
                tbl = get_progress_table()
                tbl.update_item(
                    Key={"application_id": validated["application_id"]},
                    UpdateExpression="SET callback_id = :cid",
                    ExpressionAttributeValues={":cid": callback_id},
                )

            approval_result = context.wait_for_callback(
                submit_manager_approval,
                name="manager-approval",
                config=WaitForCallbackConfig(timeout=Duration.from_minutes(30)),
            )

            if isinstance(approval_result, str):
                approval_result = json.loads(approval_result)
            logger.info(f"Manager approval result: {approval_result}")

            if not approval_result.get("approved"):
                final_result = {
                    "application_id": validated["application_id"],
                    "applicant_name": validated["applicant_name"],
                    "status": "denied",
                    "reason": approval_result.get("reason", "Manager denied the application"),
                }
                log(
                    "manager_approval", "Application denied by manager",
                    "denied", level="warn", result=final_result,
                )
                return final_result

            log("manager_approval", "Manager approved the application", "processing")

        # ── Step 5: External Fraud Check (Callback) ─────────
        # The workflow SUSPENDS here. An external Lambda (FraudCheckFunction)
        # processes the request and calls SendDurableExecutionCallbackSuccess
        # to resume this execution. Zero compute cost while waiting.
        log("fraud_check", "Requesting external fraud check service...", "processing")

        def submit_fraud_check(callback_id, _ctx):
            """Invoke the external fraud check Lambda, passing the callback_id."""
            fraud_lambda = boto3.client("lambda")
            fraud_lambda.invoke(
                FunctionName=os.environ["FRAUD_CHECK_FUNCTION"],
                InvocationType="Event",
                Payload=json.dumps({
                    "callback_id": callback_id,
                    "application_id": validated["application_id"],
                    "applicant_name": validated["applicant_name"],
                }),
            )

        fraud_result = context.wait_for_callback(
            submit_fraud_check,
            name="fraud-check",
            config=WaitForCallbackConfig(timeout=Duration.from_minutes(5)),
        )

        if isinstance(fraud_result, str):
            fraud_result = json.loads(fraud_result)
        logger.info(f"Fraud check result: {fraud_result}")
        log(
            "fraud_check",
            f"Fraud check passed — {fraud_result.get('checked_by', 'external service')}",
            "processing",
        )

        # ── Step 6: Generate Loan Offer ─────────────────────
        log("generating_offer", "Generating loan offer...", "processing")
        offer = context.step(generate_loan_offer(validated, risk))

        logger.info(
            f"Offer generated: {offer['offer_id']} — "
            f"${offer['monthly_payment']}/mo at {offer['annual_rate']}%"
        )
        log(
            "generating_offer",
            f"Offer {offer['offer_id']}: ${offer['monthly_payment']}/mo at {offer['annual_rate']}%",
            "processing",
        )

        # ── Step 6: Disburse Funds ──────────────────────────
        log("disbursing", "Disbursing funds...", "processing")
        disbursement = context.step(disburse_funds(offer))

        logger.info(f"Funds disbursed: {disbursement['disbursement_ref']}")

        final_result = {
            "application_id": validated["application_id"],
            "applicant_name": validated["applicant_name"],
            "status": "approved",
            "offer_id": offer["offer_id"],
            "loan_amount": offer["loan_amount"],
            "annual_rate": offer["annual_rate"],
            "monthly_payment": offer["monthly_payment"],
            "term_months": offer["term_months"],
            "disbursement_ref": disbursement["disbursement_ref"],
        }
        log(
            "complete", "Loan approved and funds disbursed!",
            "approved", result=final_result,
        )
        return final_result

    except Exception as error:
        logger.error(f"Loan workflow failed: {error}")
        log(
            "error", f"Workflow error: {str(error)}",
            "failed", level="error",
        )
        raise
