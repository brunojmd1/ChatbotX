import { createEnv } from "@t3-oss/env-core"
import { z } from "zod"

export const keys = () =>
  createEnv({
    server: {
      NEXT_PUBLIC_EDITION: z
        .enum(["community", "enterprise", "cloud"])
        .default("community"),
      NEXT_PUBLIC_BUILDER_URL: z.url().default("http://localhost:3123"),
      PLATFORM_ADMIN_EMAIL: z.email().optional(),
      LICENSE_KEY: z.string().optional(),
      // Shared secret authenticating server-to-server calls from UpUnity's
      // backend to the /api/internal/upunity-bridge/* routes. Unset by
      // default, which keeps the bridge routes hard-disabled (they 503
      // rather than accept an empty/undefined secret).
      UPUNITY_BRIDGE_SECRET: z.string().min(32).optional(),
    },
    runtimeEnv: process.env,
  })

export const env = keys()

export const isCommunity = () => keys().NEXT_PUBLIC_EDITION === "community"
export const isEnterprise = () => keys().NEXT_PUBLIC_EDITION === "enterprise"
export const isCloud = () => keys().NEXT_PUBLIC_EDITION === "cloud"
