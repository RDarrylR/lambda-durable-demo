/**
 * Loan API Lambda — Frontend API Layer
 * =====================================
 *
 * Single Lambda function behind API Gateway HttpApi with path-based routing.
 *
 * Endpoints:
 *   POST /apply           — Submit a loan application, invoke demo workflow async
 *   GET  /status/{id}     — Poll for workflow progress and logs
 *   POST /approve/{id}    — Manager approval callback (resume suspended workflow)
 *
 * Environment variables:
 *   PROGRESS_TABLE        — DynamoDB table name for progress tracking
 *   LOAN_FUNCTION_NAME    — ARN of the loan durable Lambda (alias)
 */

import { Logger } from "@aws-lambda-powertools/logger";
import { Tracer } from "@aws-lambda-powertools/tracer";
import { Metrics, MetricUnit } from "@aws-lambda-powertools/metrics";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  GetCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  LambdaClient,
  InvokeCommand,
  SendDurableExecutionCallbackSuccessCommand,
} from "@aws-sdk/client-lambda";

const logger = new Logger({ serviceName: "LoanApi" });
const tracer = new Tracer({ serviceName: "LoanApi" });
const metrics = new Metrics({ namespace: "LoanWorkflow", serviceName: "LoanApi" });

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const lambdaClient = new LambdaClient({});

const PROGRESS_TABLE = process.env.PROGRESS_TABLE;
const LOAN_FUNCTION_NAME = process.env.LOAN_FUNCTION_NAME;

/**
 * Generate a random string of uppercase letters and digits.
 */
function randomSuffix(length = 4) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

/**
 * Build a standard HTTP response.
 */
function response(statusCode, body) {
  return {
    statusCode,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

/**
 * POST /apply — Submit a new loan application.
 */
async function handleApply(body) {
  const segment = tracer.getSegment();
  const subsegment = segment.addNewSubsegment("## handleApply");
  tracer.setSegment(subsegment);

  try {
    const name = (body.name || "").trim();
    const address = (body.address || "").trim();
    const phone = (body.phone || "").trim();
    const sin = (body.sin || "").trim();
    const loanAmount = parseFloat(body.loan_amount);

    if (!name || !sin || !loanAmount) {
      return response(400, { error: "Missing required fields: name, sin, loan_amount" });
    }

    if (isNaN(loanAmount)) {
      return response(400, { error: "loan_amount must be a number" });
    }

    // Generate application ID
    const applicationId = `LOAN-${Math.floor(Date.now() / 1000)}-${randomSuffix()}`;
    const timestamp = new Date().toISOString();

    // Create initial DynamoDB record
    await docClient.send(
      new PutCommand({
        TableName: PROGRESS_TABLE,
        Item: {
          application_id: applicationId,
          status: "submitted",
          current_step: "submitted",
          applicant_name: name,
          loan_amount: loanAmount,
          logs: [
            {
              timestamp,
              step: "submitted",
              message: "Application received",
              level: "info",
            },
          ],
          result: null,
          created_at: timestamp,
        },
      })
    );

    // Build workflow event payload
    const workflowEvent = {
      application_id: applicationId,
      applicant_name: name,
      ssn_last4: sin,
      annual_income: 85000,
      loan_amount: loanAmount,
      loan_purpose: "personal_loan",
      address,
      phone,
    };

    // Invoke loan workflow Lambda asynchronously
    await lambdaClient.send(
      new InvokeCommand({
        FunctionName: LOAN_FUNCTION_NAME,
        InvocationType: "Event",
        Payload: Buffer.from(JSON.stringify(workflowEvent)),
      })
    );

    logger.info("Application created and workflow invoked", { applicationId });
    metrics.addMetric("ApplicationsSubmitted", MetricUnit.Count, 1);

    return response(200, { application_id: applicationId });
  } finally {
    subsegment.close();
    tracer.setSegment(segment);
  }
}

/**
 * GET /status/{applicationId} — Return current progress.
 */
async function handleStatus(applicationId) {
  const segment = tracer.getSegment();
  const subsegment = segment.addNewSubsegment("## handleStatus");
  tracer.setSegment(subsegment);

  try {
    if (!applicationId) {
      return response(400, { error: "Missing applicationId" });
    }

    const result = await docClient.send(
      new GetCommand({
        TableName: PROGRESS_TABLE,
        Key: { application_id: applicationId },
      })
    );

    if (!result.Item) {
      return response(404, { error: "Application not found" });
    }

    return response(200, result.Item);
  } finally {
    subsegment.close();
    tracer.setSegment(segment);
  }
}

/**
 * POST /approve/{applicationId} — Manager approval sends callback to resume workflow.
 */
async function handleApprove(applicationId, body) {
  const segment = tracer.getSegment();
  const subsegment = segment.addNewSubsegment("## handleApprove");
  tracer.setSegment(subsegment);

  try {
    if (!applicationId) {
      return response(400, { error: "Missing applicationId" });
    }

    const approved = body.approved || false;

    // Read callback_id from DynamoDB
    const result = await docClient.send(
      new GetCommand({
        TableName: PROGRESS_TABLE,
        Key: { application_id: applicationId },
      })
    );

    const item = result.Item;
    if (!item) {
      return response(404, { error: "Application not found" });
    }

    const callbackId = item.callback_id;
    if (!callbackId) {
      return response(400, { error: "No pending approval for this application" });
    }

    // Send callback to resume the suspended durable execution
    const callbackResult = { approved };
    if (!approved) {
      callbackResult.reason = body.reason || "Manager denied the application";
    }

    await lambdaClient.send(
      new SendDurableExecutionCallbackSuccessCommand({
        CallbackId: callbackId,
        Result: JSON.stringify(callbackResult),
      })
    );

    // Clear the callback_id from DynamoDB
    await docClient.send(
      new UpdateCommand({
        TableName: PROGRESS_TABLE,
        Key: { application_id: applicationId },
        UpdateExpression: "REMOVE callback_id",
      })
    );

    logger.info("Approval sent", { applicationId, approved });
    metrics.addMetric("ApprovalsProcessed", MetricUnit.Count, 1);

    return response(200, { status: "approval_sent", approved });
  } finally {
    subsegment.close();
    tracer.setSegment(segment);
  }
}

/**
 * Main Lambda handler — routes requests based on HTTP method and path.
 */
export const handler = async (event, context) => {
  logger.addContext(context);
  tracer.annotateColdStart();
  metrics.captureColdStartMetric();

  try {
    const method = event.requestContext?.http?.method || event.httpMethod;
    const path = event.requestContext?.http?.path || event.path || "";

    logger.info("Incoming request", { method, path });

    let body = {};
    if (event.body) {
      try {
        body = JSON.parse(event.isBase64Encoded ? Buffer.from(event.body, "base64").toString() : event.body);
      } catch {
        return response(400, { error: "Invalid JSON body" });
      }
    }

    // Route: POST /apply
    if (method === "POST" && path === "/apply") {
      return await handleApply(body);
    }

    // Route: GET /status/{applicationId}
    const statusMatch = path.match(/^\/status\/(.+)$/);
    if (method === "GET" && statusMatch) {
      return await handleStatus(statusMatch[1]);
    }

    // Route: POST /approve/{applicationId}
    const approveMatch = path.match(/^\/approve\/(.+)$/);
    if (method === "POST" && approveMatch) {
      return await handleApprove(approveMatch[1], body);
    }

    return response(404, { error: "Not found" });
  } finally {
    metrics.publishStoredMetrics();
  }
};
