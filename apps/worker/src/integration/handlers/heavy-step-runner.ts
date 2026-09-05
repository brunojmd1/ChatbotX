import { createHash } from "node:crypto"
import {
  type AIEditImageSchema,
  type AIGenerateImageSchema,
  type AISpeechToTextSchema,
  type AITextToSpeechSchema,
  aiEditImageSchema,
  aiGenerateImageSchema,
  aiSpeechToTextSchema,
  aiTextToSpeechSchema,
} from "@chatbotx.io/flow-config"
import {
  getHeavyJobCompletionWaitTimeoutMs,
  getHeavyJobOptions,
  getHeavyQueueEvents,
  getRedisConnection,
  HeavyJobAction,
  type HeavyJobData,
  heavyQueue,
  heavyStepResultSchema,
  JobCompletionStateUnknownError,
  waitForJobCompletionWithRetries,
} from "@chatbotx.io/worker-config"
import { normalizeError } from "universal-error-normalizer"
import { z } from "zod"
import { env } from "../../env"
import { logger } from "../../lib/logger"
import { saveResultToCustomField } from "../utils/contact"
import type { HeavyStepProps } from "./flow-utils"
import type { ExecuteStepResult } from "./step"

type HeavyStepRunnerAction = Extract<
  HeavyJobAction,
  "aiEditImage" | "aiGenerateImage" | "aiSpeechToText" | "aiTextToSpeech"
>

type HeavyStepJobData = Extract<HeavyJobData, { type: HeavyStepRunnerAction }>

type HeavyJobIdInput = {
  action: HeavyStepRunnerAction
  parentJobId: string
  conversationId: string
  contactInboxId: string
  stepId: string
}

type HeavyOutcomeState = "pending" | "succeeded" | "timed_out" | "writing"

const outcomeKeyPrefix = "heavy-step-outcome"
const STALE_WRITING_MS = 30_000

const outcomeStateSchema = z.object({
  status: z.enum(["pending", "succeeded", "timed_out", "writing"]),
  deadlineAt: z.number(),
  writingStartedAt: z.number().optional(),
})

type HeavyOutcomeRecord = z.infer<typeof outcomeStateSchema>

function stableJson(input: HeavyJobIdInput): string {
  return JSON.stringify({
    action: input.action,
    contactInboxId: input.contactInboxId,
    conversationId: input.conversationId,
    parentJobId: input.parentJobId,
    stepId: input.stepId,
  })
}

function hash(input: string): string {
  return createHash("sha256").update(input).digest("hex").slice(0, 32)
}

export function buildHeavyJobId(input: HeavyJobIdInput): string {
  return `heavy-${input.action}-${hash(stableJson(input))}`
}

function buildOutcomeKey(input: HeavyJobIdInput): string {
  return `${outcomeKeyPrefix}:${input.action}:${hash(stableJson(input))}`
}

function outcomeTtlMs(): number {
  return Math.max(env.HEAVY_JOB_WAIT_TIMEOUT_MS * 4, 10 * 60_000)
}

async function readOutcomeState(
  key: string,
): Promise<HeavyOutcomeRecord | null> {
  const raw = await getRedisConnection().get(key)
  if (!raw) {
    return null
  }

  try {
    const parsedJson: unknown = JSON.parse(raw)
    const parsed = outcomeStateSchema.safeParse(parsedJson)
    if (parsed.success) {
      return parsed.data
    }

    logger.warn(
      { err: normalizeError(parsed.error), key },
      "[heavy-step] Invalid outcome state in Redis",
    )
  } catch (err) {
    logger.warn(
      { err: normalizeError(err), key },
      "[heavy-step] Invalid outcome state in Redis",
    )
  }

  return null
}

async function ensurePendingOutcome(
  key: string,
  deadlineAt: number,
): Promise<HeavyOutcomeState> {
  const redis = getRedisConnection()
  await redis.set(
    key,
    JSON.stringify({ status: "pending", deadlineAt }),
    "PX",
    outcomeTtlMs(),
    "NX",
  )

  const state = await readOutcomeState(key)
  if (!state) {
    return "timed_out"
  }

  const now = Date.now()
  if (state.status === "pending" && now > state.deadlineAt) {
    return await markOutcomeTimedOut(key)
  }

  if (state.status === "writing") {
    if (now > state.deadlineAt) {
      return await markOutcomeTimedOut(key)
    }
    if (
      !state.writingStartedAt ||
      now - state.writingStartedAt > STALE_WRITING_MS
    ) {
      return "pending"
    }
  }

  return state.status
}

async function markOutcomeTimedOut(key: string): Promise<HeavyOutcomeState> {
  const result = await getRedisConnection().eval(
    `
local raw = redis.call("GET", KEYS[1])
if not raw then
  return "timed_out"
end
local obj = cjson.decode(raw)
if obj["status"] == "pending" or obj["status"] == "writing" then
  obj["status"] = "timed_out"
  redis.call("SET", KEYS[1], cjson.encode(obj), "KEEPTTL")
  return "timed_out"
end
return obj["status"]
`,
    1,
    key,
  )

  return outcomeStateSchema.shape.status.safeParse(result).data ?? "timed_out"
}

async function resetOutcomeWriteClaim(key: string): Promise<void> {
  await getRedisConnection().eval(
    `
local raw = redis.call("GET", KEYS[1])
if not raw then
  return "timed_out"
end
local obj = cjson.decode(raw)
if obj["status"] == "writing" then
  obj["status"] = "pending"
  obj["writingStartedAt"] = nil
  redis.call("SET", KEYS[1], cjson.encode(obj), "KEEPTTL")
end
return obj["status"]
`,
    1,
    key,
  )
}

async function claimOutcomeForWrite(key: string): Promise<HeavyOutcomeState> {
  const result = await getRedisConnection().eval(
    `
local raw = redis.call("GET", KEYS[1])
if not raw then
  return "timed_out"
end
local obj = cjson.decode(raw)
local now = tonumber(ARGV[1])
local staleAfterMs = tonumber(ARGV[2])
if now > tonumber(obj["deadlineAt"]) then
  if obj["status"] == "pending" or obj["status"] == "writing" then
    obj["status"] = "timed_out"
    redis.call("SET", KEYS[1], cjson.encode(obj), "KEEPTTL")
  end
  return obj["status"]
end
if obj["status"] == "pending" then
  obj["status"] = "writing"
  obj["writingStartedAt"] = now
  redis.call("SET", KEYS[1], cjson.encode(obj), "KEEPTTL")
  return "writing"
end
if obj["status"] == "writing" and (not obj["writingStartedAt"] or now - tonumber(obj["writingStartedAt"]) > staleAfterMs) then
  obj["writingStartedAt"] = now
  redis.call("SET", KEYS[1], cjson.encode(obj), "KEEPTTL")
  return "writing"
end
return obj["status"]
`,
    1,
    key,
    Date.now().toString(),
    STALE_WRITING_MS.toString(),
  )

  return outcomeStateSchema.shape.status.safeParse(result).data ?? "timed_out"
}

async function markOutcomeSucceeded(key: string): Promise<HeavyOutcomeState> {
  const result = await getRedisConnection().eval(
    `
local raw = redis.call("GET", KEYS[1])
if not raw then
  return "timed_out"
end
local obj = cjson.decode(raw)
if (obj["status"] == "pending" or obj["status"] == "writing") and tonumber(ARGV[1]) <= tonumber(obj["deadlineAt"]) then
  obj["status"] = "succeeded"
  obj["writingStartedAt"] = nil
  redis.call("SET", KEYS[1], cjson.encode(obj), "KEEPTTL")
  return "succeeded"
end
return obj["status"]
`,
    1,
    key,
    Date.now().toString(),
  )

  return outcomeStateSchema.shape.status.safeParse(result).data ?? "timed_out"
}

function buildHeavyStepJobData(
  action: HeavyStepRunnerAction,
  props: HeavyStepProps<
    | AIEditImageSchema
    | AIGenerateImageSchema
    | AISpeechToTextSchema
    | AITextToSpeechSchema
  >,
): HeavyStepJobData {
  const baseData = {
    conversationId: props.conversation.id,
    contactInboxId: props.contactInbox.id,
    metadata: props.metadata,
  }

  switch (action) {
    case HeavyJobAction.aiEditImage:
      return {
        type: HeavyJobAction.aiEditImage,
        data: { ...baseData, step: aiEditImageSchema.parse(props.step) },
      }
    case HeavyJobAction.aiGenerateImage:
      return {
        type: HeavyJobAction.aiGenerateImage,
        data: { ...baseData, step: aiGenerateImageSchema.parse(props.step) },
      }
    case HeavyJobAction.aiSpeechToText:
      return {
        type: HeavyJobAction.aiSpeechToText,
        data: { ...baseData, step: aiSpeechToTextSchema.parse(props.step) },
      }
    case HeavyJobAction.aiTextToSpeech:
      return {
        type: HeavyJobAction.aiTextToSpeech,
        data: { ...baseData, step: aiTextToSpeechSchema.parse(props.step) },
      }
    default: {
      const _exhaustive: never = action
      throw new Error(`Unsupported heavy step action: ${_exhaustive}`)
    }
  }
}

export function runViaHeavyWorker(
  action: typeof HeavyJobAction.aiEditImage,
  props: HeavyStepProps<AIEditImageSchema>,
): Promise<ExecuteStepResult>
export function runViaHeavyWorker(
  action: typeof HeavyJobAction.aiGenerateImage,
  props: HeavyStepProps<AIGenerateImageSchema>,
): Promise<ExecuteStepResult>
export function runViaHeavyWorker(
  action: typeof HeavyJobAction.aiSpeechToText,
  props: HeavyStepProps<AISpeechToTextSchema>,
): Promise<ExecuteStepResult>
export function runViaHeavyWorker(
  action: typeof HeavyJobAction.aiTextToSpeech,
  props: HeavyStepProps<AITextToSpeechSchema>,
): Promise<ExecuteStepResult>
export async function runViaHeavyWorker(
  action: HeavyStepRunnerAction,
  props: HeavyStepProps<
    | AIEditImageSchema
    | AIGenerateImageSchema
    | AISpeechToTextSchema
    | AITextToSpeechSchema
  >,
): Promise<ExecuteStepResult> {
  const idInput = {
    action,
    parentJobId: props.flowExecutionKey,
    conversationId: props.conversation.id,
    contactInboxId: props.contactInbox.id,
    stepId: props.step.id,
  }
  const outcomeKey = buildOutcomeKey(idInput)
  const completionWaitTimeoutMs = getHeavyJobCompletionWaitTimeoutMs(
    action,
    env.HEAVY_JOB_WAIT_TIMEOUT_MS,
  )
  const deadlineAt = Date.now() + completionWaitTimeoutMs
  const outcome = await ensurePendingOutcome(outcomeKey, deadlineAt)

  if (outcome === "timed_out") {
    return {
      status: "error",
      errorMessage: "Heavy step already timed out",
      result: null,
    }
  }

  if (outcome === "succeeded") {
    return { status: "success", result: null }
  }

  if (outcome === "writing") {
    return {
      status: "error",
      errorMessage: "Heavy step output write is already in progress",
      result: null,
    }
  }

  const jobId = buildHeavyJobId(idInput)
  let writeClaimed = false

  try {
    const job = await heavyQueue.add(
      action,
      buildHeavyStepJobData(action, props),
      { ...getHeavyJobOptions(action), jobId },
    )

    if (!(job && typeof job === "object" && "waitUntilFinished" in job)) {
      throw new Error("Heavy queue did not return a waitable job")
    }

    const rawResult = await waitForJobCompletionWithRetries(
      job,
      heavyQueue,
      getHeavyQueueEvents(),
      completionWaitTimeoutMs,
    )
    const result = heavyStepResultSchema.parse(rawResult)

    if (result.status === "error") {
      await markOutcomeTimedOut(outcomeKey)
      return {
        status: "error",
        errorMessage: result.errorMessage,
        result: null,
      }
    }

    if (props.step.outputFieldId) {
      const writeOutcome = await claimOutcomeForWrite(outcomeKey)
      if (writeOutcome !== "writing") {
        return {
          status: "error",
          errorMessage:
            writeOutcome === "timed_out"
              ? "Heavy step completed after its deadline"
              : "Heavy step output was already handled",
          result: null,
        }
      }

      writeClaimed = true
      await saveResultToCustomField({
        contactId: props.conversation.contactId,
        customFieldId: props.step.outputFieldId,
        fullText: result.outputValue,
        workspaceId: props.conversation.workspaceId,
        contactInboxId: props.contactInbox.id,
      })
    }

    const successOutcome = await markOutcomeSucceeded(outcomeKey)
    if (successOutcome !== "succeeded") {
      return {
        status: "error",
        errorMessage: "Heavy step completed after its deadline",
        result: null,
      }
    }

    return { status: "success", result: null }
  } catch (err) {
    const error = normalizeError(err)
    if (err instanceof JobCompletionStateUnknownError) {
      // The heavy job may still be retrying. Preserve the pending outcome and
      // let the parent integration job retry rather than discarding a result.
      throw err
    }
    if (writeClaimed) {
      // The write is an idempotent update. Returning the claim to pending lets
      // a retry recover from a DB outage or a caller crash without losing the
      // already-produced AI result.
      try {
        await resetOutcomeWriteClaim(outcomeKey)
      } catch (resetError) {
        logger.error(
          { err: normalizeError(resetError), action, jobId },
          "Failed to reset heavy step output claim",
        )
      }
      // A persistence failure must be visible to the parent worker. Returning
      // an error result would acknowledge the parent job and permanently lose
      // the already-computed provider result.
      throw err
    }
    await markOutcomeTimedOut(outcomeKey)
    logger.error(
      {
        err: error,
        action,
        conversationId: props.conversation.id,
        contactInboxId: props.contactInbox.id,
        jobId,
      },
      "Heavy step timed out or failed",
    )
    return { status: "error", errorMessage: error.message, result: null }
  }
}
