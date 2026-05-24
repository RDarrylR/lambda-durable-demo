/**
 * Loan Durable Workflow — Frontend Presentation
 * ===============================================
 *
 * Durable loan approval workflow designed for live demos with a React frontend.
 *
 * Features:
 *   - Writes progress to DynamoDB at each step (frontend polls for updates)
 *   - Hardcoded scenarios based on SIN last 4 digits for predictable outcomes
 *   - Deliberate delays in each step to visualize progress
 *   - External fraud check via callback (separate Lambda sends callback to resume)
 *   - Manager approval callback for loans >= $100K
 *
 * Hardcoded scenarios:
 *   - SIN ending 1111 (Alice): Always approved
 *   - SIN ending 2222 (Bob):   Always denied (credit score too low)
 *   - SIN ending 3333 (Charlie): Approved if loan_amount <= $25,000
 */

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { Logger } from "@aws-lambda-powertools/logger";
import { withDurableExecution } from "@aws/durable-execution-sdk-js";
import crypto from "crypto";

const logger = new Logger({ serviceName: "LoanWorkflow" });

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const PROGRESS_TABLE = process.env.PROGRESS_TABLE;
const FRAUD_CHECK_FUNCTION = process.env.FRAUD_CHECK_FUNCTION;

// ─────────────────────────────────────────────────
// Utility: sleep for simulated delays
// ─────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─────────────────────────────────────────────────
// DynamoDB Progress Logging
// ─────────────────────────────────────────────────

async function logProgress(
  applicationId,
  step,
  message,
  status,
  level = "info",
  result = null
) {
  const timestamp = new Date().toISOString();
  const logEntry = { timestamp, step, message, level };

  let updateExpr =
    "SET #logs = list_append(if_not_exists(#logs, :empty_list), :new_log), " +
    "current_step = :step, #status = :status, updated_at = :ts";
  const exprValues = {
    ":new_log": [logEntry],
    ":empty_list": [],
    ":step": step,
    ":status": status,
    ":ts": timestamp,
  };
  const exprNames = {
    "#logs": "logs",
    "#status": "status",
  };

  if (result !== null) {
    updateExpr += ", #result = :result";
    exprValues[":result"] = result;
    exprNames["#result"] = "result";
  }

  await docClient.send(
    new UpdateCommand({
      TableName: PROGRESS_TABLE,
      Key: { application_id: applicationId },
      UpdateExpression: updateExpr,
      ExpressionAttributeValues: exprValues,
      ExpressionAttributeNames: exprNames,
    })
  );
}

// ─────────────────────────────────────────────────
// Hardcoded Scenario Logic
// ─────────────────────────────────────────────────

function getScenarioDecision(ssnLast4, loanAmount) {
  if (ssnLast4 === "1111") return "approved";
  if (ssnLast4 === "2222") return "denied";
  if (ssnLast4 === "3333") return loanAmount <= 25000 ? "approved" : "denied";
  // Default: approve for unknown SINs
  return "approved";
}

// ─────────────────────────────────────────────────
// Deterministic pseudo-random using MD5 seed
// ─────────────────────────────────────────────────

function seededRandom(seed) {
  const hash = crypto.createHash("md5").update(seed).digest("hex");
  const seedNum = parseInt(hash.substring(0, 8), 16);
  // Simple seeded PRNG (Linear Congruential Generator)
  let state = seedNum;
  return function (min, max) {
    state = (state * 1664525 + 1013904223) & 0xffffffff;
    return min + (((state >>> 0) / 0xffffffff) * (max - min + 1)) | 0;
  };
}

// ─────────────────────────────────────────────────
// Durable Steps
// ─────────────────────────────────────────────────

async function validateApplication(application) {
  logger.info("Validating application", {
    applicationId: application.application_id,
  });
  await sleep(2000);

  const required = [
    "application_id",
    "applicant_name",
    "ssn_last4",
    "annual_income",
    "loan_amount",
    "loan_purpose",
  ];
  const missing = required.filter((f) => !(f in application));
  if (missing.length > 0) {
    throw new Error(`Missing required fields: ${missing.join(", ")}`);
  }

  const income = application.annual_income;
  const loanAmount = application.loan_amount;

  if (loanAmount <= 0) throw new Error("Loan amount must be positive");
  if (income <= 0) throw new Error("Annual income must be positive");

  const dtiEstimate = (loanAmount * 0.05) / (income / 12);

  return {
    application_id: application.application_id,
    applicant_name: application.applicant_name,
    ssn_last4: application.ssn_last4,
    annual_income: income,
    loan_amount: loanAmount,
    loan_purpose: application.loan_purpose,
    estimated_dti: Math.round(dtiEstimate * 100) / 100,
    status: "validated",
    validated_at: new Date().toISOString(),
  };
}

async function pullCreditReport(bureau, ssnLast4) {
  logger.info(`Pulling credit report from ${bureau}`);
  await sleep(3000);

  const rng = seededRandom(`${bureau}-${ssnLast4}`);
  const score = rng(580, 820);
  const derogatoryMarks = rng(0, 3);
  const openAccounts = rng(2, 15);

  return {
    bureau,
    score,
    report_id: `${bureau.substring(0, 3).toUpperCase()}-${ssnLast4}-${score}`,
    derogatory_marks: derogatoryMarks,
    open_accounts: openAccounts,
    pulled_at: new Date().toISOString(),
  };
}

async function calculateRiskScore(creditReports, ssnLast4, loanAmount) {
  logger.info("Calculating risk score from credit reports");
  await sleep(2000);

  const scores = creditReports.map((r) => r.score);
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
  const totalDerogatory = creditReports.reduce(
    (sum, r) => sum + r.derogatory_marks,
    0
  );

  let tier, baseRate;
  if (avgScore >= 740 && totalDerogatory === 0) {
    tier = "prime";
    baseRate = 5.25;
  } else if (avgScore >= 670) {
    tier = "near-prime";
    baseRate = 7.5;
  } else if (avgScore >= 580) {
    tier = "subprime";
    baseRate = 11.0;
  } else {
    tier = "deep-subprime";
    baseRate = 15.0;
  }

  // Apply hardcoded scenario override
  const decision = getScenarioDecision(ssnLast4, loanAmount);

  return {
    average_score: Math.round(avgScore * 10) / 10,
    min_score: Math.min(...scores),
    max_score: Math.max(...scores),
    total_derogatory_marks: totalDerogatory,
    risk_tier: tier,
    base_rate: baseRate,
    decision,
  };
}

async function generateLoanOffer(app, risk) {
  logger.info(`Generating offer for ${app.application_id}`);
  await sleep(2000);

  const rate = risk.base_rate;
  const loanAmount = app.loan_amount;
  const termMonths = 60;
  const monthlyRate = rate / 100 / 12;

  let payment;
  if (monthlyRate > 0) {
    payment =
      (loanAmount *
        (monthlyRate * Math.pow(1 + monthlyRate, termMonths))) /
      (Math.pow(1 + monthlyRate, termMonths) - 1);
  } else {
    payment = loanAmount / termMonths;
  }

  const offerId = crypto
    .createHash("sha256")
    .update(`offer-${app.application_id}-${rate}`)
    .digest("hex")
    .substring(0, 10);

  return {
    offer_id: `OFFER-${offerId.toUpperCase()}`,
    application_id: app.application_id,
    loan_amount: loanAmount,
    annual_rate: rate,
    term_months: termMonths,
    monthly_payment: Math.round(payment * 100) / 100,
    total_interest: Math.round((payment * termMonths - loanAmount) * 100) / 100,
    status: "offer_generated",
    generated_at: new Date().toISOString(),
  };
}

async function disburseFunds(offer) {
  logger.info(
    `Disbursing $${offer.loan_amount.toLocaleString()} for ${offer.offer_id}`
  );
  await sleep(2000);

  return {
    offer_id: offer.offer_id,
    disbursement_ref: `DSB-${offer.offer_id.slice(-6)}`,
    amount_disbursed: offer.loan_amount,
    status: "funded",
    funded_at: new Date().toISOString(),
  };
}

// ─────────────────────────────────────────────────
// Main Durable Execution Handler
// ─────────────────────────────────────────────────

export const handler = withDurableExecution(async (event, context) => {
  const applicationId = event.application_id;
  logger.appendKeys({ applicationId });

  // Detect replay: count how many log entries exist per step in DynamoDB.
  const existing = await docClient.send(
    new GetCommand({
      TableName: PROGRESS_TABLE,
      Key: { application_id: applicationId },
    })
  );
  const item = existing.Item || {};
  const priorCounts = {};
  for (const entry of item.logs || []) {
    priorCounts[entry.step] = (priorCounts[entry.step] || 0) + 1;
  }
  const callCounts = {};

  async function log(step, message, status, level = "info", result = null) {
    callCounts[step] = (callCounts[step] || 0) + 1;
    if (callCounts[step] <= (priorCounts[step] || 0)) {
      message = `[REPLAY] ${message}`;
      level = "replay";
    }
    await logProgress(applicationId, step, message, status, level, result);
  }

  try {
    // ── Step 1: Validate Application ────────────────────
    await log("validating", "Validating loan application...", "processing");
    const validated = await context.step(
      "validate-application",
      async () => validateApplication(event)
    );

    logger.info(
      `Application validated: ${validated.application_id} — $${validated.loan_amount.toLocaleString()} (${validated.loan_purpose})`
    );
    await log("validating", "Application validated successfully", "processing");

    // ── Step 2: Parallel Credit Bureau Checks ───────────
    await log(
      "credit_check",
      "Pulling credit reports from 3 bureaus...",
      "processing"
    );
    const bureaus = ["equifax", "transunion", "experian"];

    const creditResults = await context.parallel(
      "pull-credit-reports",
      bureaus,
      async (ctx, bureau) => {
        return ctx.step(`pull-${bureau}`, async () =>
          pullCreditReport(bureau, validated.ssn_last4)
        );
      }
    );

    const creditReports = creditResults.getResults();
    const scoresStr = creditReports
      .map((r) => `${r.bureau}=${r.score}`)
      .join(", ");
    logger.info(`Credit reports pulled — scores: ${scoresStr}`);
    await log(
      "credit_check",
      `Credit scores received: ${scoresStr}`,
      "processing"
    );

    // ── Step 3: Risk Assessment ─────────────────────────
    await log("risk_assessment", "Calculating risk score...", "processing");
    const risk = await context.step("calculate-risk-score", async () =>
      calculateRiskScore(creditReports, validated.ssn_last4, validated.loan_amount)
    );

    logger.info(
      `Risk assessed: tier=${risk.risk_tier}, avg=${risk.average_score}, decision=${risk.decision}`
    );
    await log(
      "risk_assessment",
      `Risk tier: ${risk.risk_tier}, avg score: ${risk.average_score}, decision: ${risk.decision}`,
      "processing"
    );

    // ── Denied Path ─────────────────────────────────────
    if (risk.decision === "denied") {
      const finalResult = {
        application_id: validated.application_id,
        applicant_name: validated.applicant_name,
        status: "denied",
        reason: `Application denied — risk tier: ${risk.risk_tier}, avg credit score: ${risk.average_score}`,
        risk_tier: risk.risk_tier,
        average_score: risk.average_score,
      };
      await log(
        "risk_assessment",
        "Application denied",
        "denied",
        "warn",
        finalResult
      );
      return finalResult;
    }

    // ── Step 4: Manager Approval (if >= $100,000) ────────
    if (validated.loan_amount >= 100000) {
      await log(
        "manager_approval",
        `Manager approval required for loans >= $100,000 (requested: $${validated.loan_amount.toLocaleString()})`,
        "pending_approval"
      );

      const approvalResult = await context.waitForCallback(
        "manager-approval",
        async (callbackId) => {
          // Store callback_id in DynamoDB so the frontend can send the approval
          await docClient.send(
            new UpdateCommand({
              TableName: PROGRESS_TABLE,
              Key: { application_id: validated.application_id },
              UpdateExpression: "SET callback_id = :cid",
              ExpressionAttributeValues: { ":cid": callbackId },
            })
          );
        },
        { timeout: { minutes: 30 } }
      );

      const parsedApproval =
        typeof approvalResult === "string"
          ? JSON.parse(approvalResult)
          : approvalResult;
      logger.info("Manager approval result", { approvalResult: parsedApproval });

      if (!parsedApproval.approved) {
        const finalResult = {
          application_id: validated.application_id,
          applicant_name: validated.applicant_name,
          status: "denied",
          reason:
            parsedApproval.reason || "Manager denied the application",
        };
        await log(
          "manager_approval",
          "Application denied by manager",
          "denied",
          "warn",
          finalResult
        );
        return finalResult;
      }

      await log(
        "manager_approval",
        "Manager approved the application",
        "processing"
      );
    }

    // ── Step 5: External Fraud Check (Callback) ─────────
    await log(
      "fraud_check",
      "Requesting external fraud check service...",
      "processing"
    );

    const fraudResult = await context.waitForCallback(
      "fraud-check",
      async (callbackId) => {
        // Invoke the external fraud check Lambda, passing the callback_id
        const lambdaClient = new LambdaClient({});
        await lambdaClient.send(
          new InvokeCommand({
            FunctionName: FRAUD_CHECK_FUNCTION,
            InvocationType: "Event",
            Payload: Buffer.from(
              JSON.stringify({
                callback_id: callbackId,
                application_id: validated.application_id,
                applicant_name: validated.applicant_name,
              })
            ),
          })
        );
      },
      { timeout: { minutes: 5 } }
    );

    const parsedFraud =
      typeof fraudResult === "string" ? JSON.parse(fraudResult) : fraudResult;
    logger.info("Fraud check result", { fraudResult: parsedFraud });
    await log(
      "fraud_check",
      `Fraud check passed — ${parsedFraud.checked_by || "external service"}`,
      "processing"
    );

    // ── Step 6: Generate Loan Offer ─────────────────────
    await log("generating_offer", "Generating loan offer...", "processing");
    const offer = await context.step("generate-loan-offer", async () =>
      generateLoanOffer(validated, risk)
    );

    logger.info(
      `Offer generated: ${offer.offer_id} — $${offer.monthly_payment}/mo at ${offer.annual_rate}%`
    );
    await log(
      "generating_offer",
      `Offer ${offer.offer_id}: $${offer.monthly_payment}/mo at ${offer.annual_rate}%`,
      "processing"
    );

    // ── Step 7: Disburse Funds ──────────────────────────
    await log("disbursing", "Disbursing funds...", "processing");
    const disbursement = await context.step("disburse-funds", async () =>
      disburseFunds(offer)
    );

    logger.info(`Funds disbursed: ${disbursement.disbursement_ref}`);

    const finalResult = {
      application_id: validated.application_id,
      applicant_name: validated.applicant_name,
      status: "approved",
      offer_id: offer.offer_id,
      loan_amount: offer.loan_amount,
      annual_rate: offer.annual_rate,
      monthly_payment: offer.monthly_payment,
      term_months: offer.term_months,
      disbursement_ref: disbursement.disbursement_ref,
    };
    await log(
      "complete",
      "Loan approved and funds disbursed!",
      "approved",
      "info",
      finalResult
    );
    return finalResult;
  } catch (error) {
    logger.error(`Loan workflow failed: ${error.message}`);
    await log(
      "error",
      `Workflow error: ${error.message}`,
      "failed",
      "error"
    );
    throw error;
  }
});
