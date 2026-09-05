import {
  defaultWorkerOptions,
  getRedisConnection,
  HeavyJobAction,
  type HeavyJobData,
  heavyJobDataSchema,
  heavyStepResultSchema,
  queueNames,
} from "@chatbotx.io/worker-config"
import { type Job, Worker } from "bullmq"
import { normalizeError } from "universal-error-normalizer"
import { env } from "../env"
import { ensureBootstrapped } from "../lib/bootstrap"
import { detectConversationAndContactInbox } from "../lib/db"
import { recordHeavyMetric } from "../lib/heavy-metrics"
import { isBlockedWorkspace } from "../lib/is-blocked-workspace"
import { logger } from "../lib/logger"
import { resolveWorkspaceId } from "../lib/resolve-workspace-id"
import { runJobWithAuditContext } from "../lib/run-job-with-audit-context"
import { analyzeImage } from "./handlers/analyze-image"
import { editImageOutput } from "./handlers/edit-image"
import {
  isExpectedHeavyStepError,
  isRetryableHeavyError,
} from "./handlers/errors"
import { extractFallbackTextSnippets } from "./handlers/extract-text-from-file"
import { generateImageOutput } from "./handlers/generate-image"
import { processAIFile } from "./handlers/process-ai-file"
import { recordHeavyAIStepProviderError } from "./handlers/provider-error"
import { speechToTextOutput } from "./handlers/speech-to-text"
import { textToSpeechOutput } from "./handlers/text-to-speech"
import { waitForHeavyProviderSlot } from "./services/provider-rate-limiter"

type HeavyStepJobData = Extract<
  HeavyJobData,
  {
    type:
      | typeof HeavyJobAction.aiEditImage
      | typeof HeavyJobAction.aiGenerateImage
      | typeof HeavyJobAction.aiSpeechToText
      | typeof HeavyJobAction.aiTextToSpeech
  }
>

function getProviderForRateLimit(jobData: HeavyJobData): string | undefined {
  if ("step" in jobData.data) {
    return jobData.data.step.provider
  }
  if (jobData.type === HeavyJobAction.analyzeImage) {
    return "provider" in jobData.data.providerInfo
      ? jobData.data.providerInfo.provider
      : "openaiCompatible"
  }
}

async function runHeavyStep(jobData: HeavyStepJobData, job: Job<HeavyJobData>) {
  const { conversation, contactInbox } =
    await detectConversationAndContactInbox({
      conversationId: jobData.data.conversationId,
      contactInboxId: jobData.data.contactInboxId,
    })

  const props = {
    conversation,
    contactInbox,
    metadata: jobData.data.metadata,
    step: jobData.data.step,
  }

  try {
    switch (jobData.type) {
      case HeavyJobAction.aiEditImage:
        return {
          status: "success" as const,
          outputValue: await editImageOutput({
            ...props,
            step: jobData.data.step,
          }),
        }
      case HeavyJobAction.aiGenerateImage:
        return {
          status: "success" as const,
          outputValue: await generateImageOutput({
            ...props,
            step: jobData.data.step,
          }),
        }
      case HeavyJobAction.aiSpeechToText:
        return {
          status: "success" as const,
          outputValue: await speechToTextOutput({
            ...props,
            step: jobData.data.step,
          }),
        }
      case HeavyJobAction.aiTextToSpeech:
        return {
          status: "success" as const,
          outputValue: await textToSpeechOutput({
            ...props,
            step: jobData.data.step,
          }),
        }
      default: {
        const _exhaustive: never = jobData
        throw new Error(`Unhandled heavy step data: ${_exhaustive}`)
      }
    }
  } catch (err) {
    const error = normalizeError(err)
    await recordHeavyAIStepProviderError({
      provider: jobData.data.step.provider,
      workspaceId: conversation.workspaceId,
      contactId: conversation.contactId,
      error: err,
    })

    logger.error(
      {
        err: error,
        conversationId: conversation.id,
        contactInboxId: contactInbox.id,
        jobId: job.id,
        jobType: jobData.type,
        workspaceId: conversation.workspaceId,
        retryable: isRetryableHeavyError(err),
      },
      "Heavy step failed",
    )

    if (!isExpectedHeavyStepError(err) && isRetryableHeavyError(err)) {
      throw err
    }

    return { status: "error" as const, errorMessage: error.message }
  }
}

async function startHeavyWorker() {
  try {
    await ensureBootstrapped()
    logger.info("Heavy worker bootstrapped successfully")
  } catch (err) {
    logger.error(
      { err: normalizeError(err) },
      "Failed to bootstrap Heavy worker",
    )
    process.exit(1)
  }

  const worker = new Worker(
    queueNames.enum.heavy,
    async (job: Job<HeavyJobData>) => {
      logger.info(job.data, `Heavy worker received job: ${job.id}`)

      const jobData = heavyJobDataSchema.parse(job.data)
      const startedAt = performance.now()
      const provider = getProviderForRateLimit(jobData)
      const queueWaitMs = Math.max(0, Date.now() - job.timestamp)
      recordHeavyMetric({
        action: jobData.type,
        attempts: job.attemptsMade,
        event: "received",
        provider,
        queueWaitMs,
      })
      recordHeavyMetric({
        action: jobData.type,
        attempts: job.attemptsMade,
        event: "started",
        provider,
        queueWaitMs,
      })
      const workspaceId = await resolveWorkspaceId(jobData.data)
      if (await isBlockedWorkspace(workspaceId)) {
        return
      }

      if (provider && env.HEAVY_PROVIDER_MIN_INTERVAL_MS > 0) {
        await waitForHeavyProviderSlot({
          minIntervalMs: env.HEAVY_PROVIDER_MIN_INTERVAL_MS,
          operation: jobData.type,
          provider,
        })
      }

      const providerStartedAt = provider ? performance.now() : undefined
      const result = await runJobWithAuditContext(
        { workspaceId, source: `heavy:${jobData.type}` },
        async () => {
          switch (jobData.type) {
            case HeavyJobAction.processAIFile:
              await processAIFile(jobData.data)
              return
            case HeavyJobAction.aiEditImage:
            case HeavyJobAction.aiGenerateImage:
            case HeavyJobAction.aiSpeechToText:
            case HeavyJobAction.aiTextToSpeech:
              return await runHeavyStep(jobData, job)
            case HeavyJobAction.extractTextFromFile:
              return await extractFallbackTextSnippets(jobData.data)
            case HeavyJobAction.analyzeImage:
              return await analyzeImage(jobData.data)
            default: {
              const _exhaustive: never = jobData
              logger.warn(
                { data: _exhaustive, jobName: job.name },
                "Unhandled heavy job type",
              )
              return
            }
          }
        },
      )

      const stepResult = heavyStepResultSchema.safeParse(result)
      recordHeavyMetric({
        action: jobData.type,
        attempts: job.attemptsMade,
        durationMs: Math.round(performance.now() - startedAt),
        event: "completed",
        outcome:
          stepResult.success && stepResult.data.status === "error"
            ? "expected_error"
            : "completed",
        providerLatencyMs:
          providerStartedAt === undefined
            ? undefined
            : Math.round(performance.now() - providerStartedAt),
        provider,
      })
      return result
    },
    {
      connection: getRedisConnection(),
      ...defaultWorkerOptions,
      concurrency: env.HEAVY_WORKER_CONCURRENCY,
      // AI provider calls are allowed to run for HEAVY_JOB_WAIT_TIMEOUT_MS;
      // keep the BullMQ lock alive for the whole budget plus a small handoff
      // margin so a slow provider cannot be redelivered while still running.
      lockDuration: Math.max(
        env.HEAVY_JOB_WAIT_TIMEOUT_MS + 60_000,
        5 * 60_000,
      ),
      stalledInterval: 60_000,
      maxStalledCount: 1,
    },
  )

  worker.on("failed", async (job, err) => {
    if (!job) {
      logger.error(
        { err: normalizeError(err) },
        "Heavy job failed without job context",
      )
      return
    }

    const parsedJobData = heavyJobDataSchema.safeParse(job.data)
    let workspaceId: string | undefined
    let workspaceResolutionError: ReturnType<typeof normalizeError> | undefined
    if (parsedJobData.success) {
      try {
        workspaceId = await resolveWorkspaceId(parsedJobData.data.data)
      } catch (resolutionError) {
        workspaceResolutionError = normalizeError(resolutionError)
      }
    }

    logger.error(
      {
        err: normalizeError(err),
        jobId: job.id,
        jobName: job.name,
        jobType: parsedJobData.success ? parsedJobData.data.type : undefined,
        workspaceId,
        workspaceResolutionError,
      },
      "Heavy job failed",
    )
    recordHeavyMetric({
      action: parsedJobData.success ? parsedJobData.data.type : undefined,
      attempts: job.attemptsMade,
      event: "failed",
      outcome: isRetryableHeavyError(err) ? "retryable_failed" : "failed",
    })
  })

  worker.on("stalled", (jobId) => {
    recordHeavyMetric({ event: "stalled", outcome: "retryable_failed" })
    logger.warn({ jobId }, "Heavy job stalled")
  })

  let isShuttingDown = false
  async function shutdown() {
    if (isShuttingDown) {
      return
    }
    isShuttingDown = true
    try {
      await worker.close()
      process.exit(0)
    } catch (err) {
      logger.error(
        { err: normalizeError(err) },
        "[HeavyWorker] Error during shutdown",
      )
      process.exit(1)
    }
  }
  process.once("SIGINT", shutdown)
  process.once("SIGTERM", shutdown)
}

startHeavyWorker().catch((err) => {
  logger.error({ err: normalizeError(err) }, "Failed to start Heavy worker")
  process.exit(1)
})
