/**
 * External Fraud Check Service — Callback Simulator
 * ===================================================
 *
 * Simulates an external fraud-check API. The demo durable workflow
 * invokes this Lambda asynchronously, passing a callback_id.
 * After a short delay (simulating processing), this function calls
 * SendDurableExecutionCallbackSuccess to resume the suspended workflow.
 *
 * This demonstrates the callback pattern: the durable execution suspends
 * with zero compute cost while an external system does work, then
 * resumes when the callback arrives.
 */

import { LambdaClient } from "@aws-sdk/client-lambda";
import { Logger } from "@aws-lambda-powertools/logger";
import { Tracer } from "@aws-lambda-powertools/tracer";

const logger = new Logger({ serviceName: "FraudCheck" });
const tracer = new Tracer({ serviceName: "FraudCheck" });

const lambdaClient = new LambdaClient({});

const WORKFLOW_FUNCTION_NAME = process.env.WORKFLOW_FUNCTION_NAME;

/**
 * Utility: sleep for simulated delays.
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export const handler = async (event, context) => {
  logger.addContext(context);
  const segment = tracer.getSegment();
  const handlerSegment = segment.addNewSubsegment("## handler");
  tracer.setSegment(handlerSegment);

  try {
    const callbackId = event.callback_id;
    const applicantName = event.applicant_name || "Unknown";
    const applicationId = event.application_id || "Unknown";

    logger.info("Fraud check started", {
      applicantName,
      applicationId,
      callbackId,
    });

    // Simulate external processing time
    await sleep(5000);

    const result = {
      fraud_check: "passed",
      risk_indicators: 0,
      checked_by: "FraudCheckService-v2",
    };

    logger.info(
      "Fraud check passed — sending callback to resume workflow",
      { applicationId, result }
    );

    // Resume the suspended durable execution
    await lambdaClient.sendDurableExecutionCallbackSuccess({
      CallbackId: callbackId,
      Result: JSON.stringify(result),
    });

    return {
      status: "callback_sent",
      callback_id: callbackId,
      application_id: applicationId,
    };
  } finally {
    handlerSegment.close();
    tracer.setSegment(segment);
  }
};
