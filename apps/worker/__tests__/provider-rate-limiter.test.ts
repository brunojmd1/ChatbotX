import { beforeEach, describe, expect, test, vi } from "vitest"

const mocks = vi.hoisted(() => ({
  eval: vi.fn(),
}))

vi.mock("@chatbotx.io/worker-config", () => ({
  getRedisConnection: () => ({ eval: mocks.eval }),
}))

const { waitForHeavyProviderSlot } = await import(
  "../src/heavy/services/provider-rate-limiter"
)

describe("waitForHeavyProviderSlot", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  test("claims a provider operation slot atomically", async () => {
    mocks.eval.mockResolvedValueOnce(0)

    await waitForHeavyProviderSlot({
      minIntervalMs: 250,
      operation: "aiGenerateImage",
      provider: "openai",
    })

    expect(mocks.eval).toHaveBeenCalledWith(
      expect.stringContaining('redis.call("GET", KEYS[1])'),
      1,
      "heavy-provider-rate:openai:aiGenerateImage",
      expect.any(String),
      "250",
    )
  })

  test("waits and retries when another worker owns the slot", async () => {
    mocks.eval.mockResolvedValueOnce(1).mockResolvedValueOnce(0)

    await waitForHeavyProviderSlot({
      minIntervalMs: 1,
      operation: "aiAnalyzeImage",
      provider: "openai",
    })

    expect(mocks.eval).toHaveBeenCalledTimes(2)
  })
})
