import { aiGenerateImageDefaultFn } from "@chatbotx.io/flow-config"
import { beforeEach, describe, expect, test, vi } from "vitest"
import type { HeavyStepProps } from "../src/integration/handlers/flow-utils"

const mocks = vi.hoisted(() => ({
  getHeavyQueueEvents: vi.fn(() => ({})),
  heavyQueueAdd: vi.fn(),
  heavyQueueGetJob: vi.fn(),
  redis: {
    eval: vi.fn(),
    get: vi.fn(),
    set: vi.fn(),
  },
  saveResultToCustomField: vi.fn(),
}))

const redisState = new Map<string, string>()

vi.mock("@chatbotx.io/worker-config", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@chatbotx.io/worker-config")>()
  return {
    ...actual,
    getHeavyQueueEvents: mocks.getHeavyQueueEvents,
    getRedisConnection: () => mocks.redis,
    heavyQueue: {
      add: mocks.heavyQueueAdd,
      getJob: mocks.heavyQueueGetJob,
    },
  }
})

vi.mock("../src/env", () => ({
  env: { HEAVY_JOB_WAIT_TIMEOUT_MS: 120_000 },
}))

vi.mock("../src/integration/utils/contact", () => ({
  saveResultToCustomField: mocks.saveResultToCustomField,
}))

vi.mock("../src/lib/logger", () => ({
  logger: { error: vi.fn(), warn: vi.fn() },
}))

const { HeavyJobAction } = await import("@chatbotx.io/worker-config")
const { buildHeavyJobId, runViaHeavyWorker } = await import(
  "../src/integration/handlers/heavy-step-runner"
)

function makeProps(
  overrides: Partial<{
    contactInboxId: string
    conversationId: string
    flowExecutionKey: string
    stepId: string
  }> = {},
) {
  const conversationId = overrides.conversationId ?? "conversation-1"
  const contactInboxId = overrides.contactInboxId ?? "contact-inbox-1"

  return {
    conversation: {
      id: conversationId,
      contactId: "contact-1",
      workspaceId: "workspace-1",
    },
    contactInbox: {
      id: contactInboxId,
    },
    flowExecutionKey: overrides.flowExecutionKey ?? "parent-job-1",
    step: aiGenerateImageDefaultFn({
      id: overrides.stepId ?? "1",
      prompt: "A quiet workspace",
      outputFieldId: "custom-field-1",
    }),
  } as HeavyStepProps<ReturnType<typeof aiGenerateImageDefaultFn>>
}

function buildOutcomeKey(props: ReturnType<typeof makeProps>) {
  const action = HeavyJobAction.aiGenerateImage
  const jobIdPrefix = `heavy-${action}-`
  const jobId = buildHeavyJobId({
    action,
    parentJobId: props.flowExecutionKey,
    conversationId: props.conversation.id,
    contactInboxId: props.contactInbox.id,
    stepId: props.step.id,
  })

  return `heavy-step-outcome:${action}:${jobId.slice(jobIdPrefix.length)}`
}

beforeEach(() => {
  vi.clearAllMocks()
  redisState.clear()
  mocks.heavyQueueGetJob.mockReset()

  mocks.redis.get.mockImplementation((key: string) =>
    Promise.resolve(redisState.get(key) ?? null),
  )
  mocks.redis.set.mockImplementation(
    (key: string, value: string, _px: string, _ttl: number, nx: string) => {
      if (nx === "NX" && redisState.has(key)) {
        return Promise.resolve(null)
      }
      redisState.set(key, value)
      return Promise.resolve("OK")
    },
  )
  mocks.redis.eval.mockImplementation(
    (script: string, _keys: number, key: string) => {
      const raw = redisState.get(key)
      if (!raw) {
        return Promise.resolve("timed_out")
      }
      const parsed = JSON.parse(raw) as {
        status: "pending" | "succeeded" | "timed_out" | "writing"
        deadlineAt: number
        writingStartedAt?: number
      }

      if (script.includes("staleAfterMs")) {
        if (parsed.status === "pending") {
          parsed.status = "writing"
          parsed.writingStartedAt = Date.now()
          redisState.set(key, JSON.stringify(parsed))
          return Promise.resolve("writing")
        }
        if (
          parsed.status === "writing" &&
          (!parsed.writingStartedAt ||
            Date.now() - parsed.writingStartedAt > 30_000)
        ) {
          parsed.writingStartedAt = Date.now()
          redisState.set(key, JSON.stringify(parsed))
          return Promise.resolve("writing")
        }
        return Promise.resolve(parsed.status)
      }

      if (script.includes('obj["status"] = "succeeded"')) {
        if (parsed.status === "pending" || parsed.status === "writing") {
          parsed.status = "succeeded"
          parsed.writingStartedAt = undefined
          redisState.set(key, JSON.stringify(parsed))
          return Promise.resolve("succeeded")
        }
        return Promise.resolve(parsed.status)
      }

      if (
        script.includes('obj["status"] = "pending"') &&
        parsed.status === "writing"
      ) {
        parsed.status = "pending"
        parsed.writingStartedAt = undefined
        redisState.set(key, JSON.stringify(parsed))
        return Promise.resolve("pending")
      }

      if (
        script.includes('obj["status"] = "timed_out"') &&
        (parsed.status === "pending" || parsed.status === "writing")
      ) {
        parsed.status = "timed_out"
        redisState.set(key, JSON.stringify(parsed))
        return Promise.resolve("timed_out")
      }

      if (script.includes("deadlineAt") && parsed.status === "pending") {
        parsed.status = "succeeded"
        redisState.set(key, JSON.stringify(parsed))
        return Promise.resolve("succeeded")
      }

      return Promise.resolve(parsed.status)
    },
  )
})

describe("runViaHeavyWorker", () => {
  test("writes the output value only after a successful bounded wait", async () => {
    const waitUntilFinished = vi.fn(async () => ({
      status: "success",
      outputValue: "https://cdn.example.com/result.png",
    }))
    mocks.heavyQueueAdd.mockResolvedValue({ waitUntilFinished })

    const result = await runViaHeavyWorker(
      HeavyJobAction.aiGenerateImage,
      makeProps(),
    )

    expect(result).toEqual({ status: "success", result: null })
    expect(mocks.saveResultToCustomField).toHaveBeenCalledWith({
      contactId: "contact-1",
      customFieldId: "custom-field-1",
      fullText: "https://cdn.example.com/result.png",
      workspaceId: "workspace-1",
      contactInboxId: "contact-inbox-1",
    })
    expect(waitUntilFinished).toHaveBeenCalledWith({}, 330_000)
    const options = mocks.heavyQueueAdd.mock.calls[0]?.[2]
    expect(options.jobId).not.toContain(":")
  })

  test("waits for a later heavy retry before returning success", async () => {
    const waitUntilFinished = vi
      .fn()
      .mockRejectedValueOnce(new Error("provider unavailable"))
      .mockResolvedValueOnce({
        status: "success",
        outputValue: "https://cdn.example.com/result.png",
      })
    mocks.heavyQueueAdd.mockResolvedValue({
      id: "heavy-job-1",
      waitUntilFinished,
      attemptsMade: 0,
      opts: { attempts: 2 },
    })
    mocks.heavyQueueGetJob.mockResolvedValue({
      attemptsMade: 1,
      opts: { attempts: 2 },
    })

    const result = await runViaHeavyWorker(
      HeavyJobAction.aiGenerateImage,
      makeProps(),
    )

    expect(result).toEqual({ status: "success", result: null })
    expect(waitUntilFinished).toHaveBeenCalledTimes(2)
    expect(waitUntilFinished.mock.calls[0]?.[1]).toBeGreaterThan(120_000)
    expect(mocks.saveResultToCustomField).toHaveBeenCalledOnce()
  })

  test("retries the parent job when Redis cannot confirm heavy retry state", async () => {
    const waitUntilFinished = vi
      .fn()
      .mockRejectedValue(new Error("provider unavailable"))
    mocks.heavyQueueAdd.mockResolvedValue({
      id: "heavy-job-1",
      waitUntilFinished,
      attemptsMade: 0,
      opts: { attempts: 2 },
    })
    mocks.heavyQueueGetJob.mockRejectedValue(new Error("Redis unavailable"))
    const props = makeProps()

    await expect(
      runViaHeavyWorker(HeavyJobAction.aiGenerateImage, props),
    ).rejects.toMatchObject({ name: "JobCompletionStateUnknownError" })

    const outcome = JSON.parse(redisState.get(buildOutcomeKey(props)) ?? "{}")
    expect(outcome.status).toBe("pending")
    expect(mocks.saveResultToCustomField).not.toHaveBeenCalled()
  })

  test("does not write after the final heavy attempt fails", async () => {
    const waitUntilFinished = vi.fn(() =>
      Promise.reject(new Error("timed out")),
    )
    mocks.heavyQueueAdd.mockResolvedValue({
      id: "heavy-job-1",
      waitUntilFinished,
      attemptsMade: 1,
      opts: { attempts: 2 },
    })
    mocks.heavyQueueGetJob.mockResolvedValue({
      attemptsMade: 2,
      opts: { attempts: 2 },
    })
    const props = makeProps()

    const result = await runViaHeavyWorker(
      HeavyJobAction.aiGenerateImage,
      props,
    )

    expect(result).toEqual({
      status: "error",
      errorMessage: "timed out",
      result: null,
    })
    expect(mocks.saveResultToCustomField).not.toHaveBeenCalled()
    expect(mocks.heavyQueueAdd).toHaveBeenCalledTimes(1)
  })

  test("rejects invalid heavy result shapes without writing", async () => {
    mocks.heavyQueueAdd.mockResolvedValue({
      waitUntilFinished: vi.fn(async () => ({ status: "success" })),
    })

    const result = await runViaHeavyWorker(
      HeavyJobAction.aiGenerateImage,
      makeProps(),
    )

    expect(result.status).toBe("error")
    expect(mocks.saveResultToCustomField).not.toHaveBeenCalled()
  })

  test("retries a failed output write without losing the heavy result", async () => {
    mocks.heavyQueueAdd.mockResolvedValue({
      waitUntilFinished: vi.fn(async () => ({
        status: "success",
        outputValue: "https://cdn.example.com/result.png",
      })),
    })
    mocks.saveResultToCustomField.mockRejectedValueOnce(new Error("db down"))
    const props = makeProps()

    const firstResult = runViaHeavyWorker(HeavyJobAction.aiGenerateImage, props)
    await expect(firstResult).rejects.toThrow("db down")
    const retryResult = await runViaHeavyWorker(
      HeavyJobAction.aiGenerateImage,
      props,
    )

    expect(retryResult).toEqual({
      status: "success",
      result: null,
    })
    expect(mocks.saveResultToCustomField).toHaveBeenCalledTimes(2)
  })

  test("reclaims stale writing outcomes and retries the output write", async () => {
    const props = makeProps()
    redisState.set(
      buildOutcomeKey(props),
      JSON.stringify({
        status: "writing",
        deadlineAt: Date.now() + 120_000,
        writingStartedAt: Date.now() - 31_000,
      }),
    )
    mocks.heavyQueueAdd.mockResolvedValue({
      waitUntilFinished: vi.fn(async () => ({
        status: "success",
        outputValue: "https://cdn.example.com/result.png",
      })),
    })

    const result = await runViaHeavyWorker(
      HeavyJobAction.aiGenerateImage,
      props,
    )

    expect(result).toEqual({ status: "success", result: null })
    expect(mocks.saveResultToCustomField).toHaveBeenCalledWith({
      contactId: "contact-1",
      customFieldId: "custom-field-1",
      fullText: "https://cdn.example.com/result.png",
      workspaceId: "workspace-1",
      contactInboxId: "contact-inbox-1",
    })
  })
})

describe("buildHeavyJobId", () => {
  test("hashes the full tuple into a BullMQ-safe id", () => {
    const base = {
      action: HeavyJobAction.aiGenerateImage,
      parentJobId: "parent:job:1",
      conversationId: "conversation-1",
      contactInboxId: "contact-inbox-1",
      stepId: "1",
    }

    const id = buildHeavyJobId(base)
    const otherConversationId = buildHeavyJobId({
      ...base,
      conversationId: "conversation-2",
    })
    const otherContactInboxId = buildHeavyJobId({
      ...base,
      contactInboxId: "contact-inbox-2",
    })

    expect(id).not.toContain(":")
    expect(otherConversationId).not.toBe(id)
    expect(otherContactInboxId).not.toBe(id)
  })
})
