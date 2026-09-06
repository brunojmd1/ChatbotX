import { db } from "@chatbotx.io/database/client"
import {
  sessionModel,
  userModel,
  verificationModel,
} from "@chatbotx.io/database/schema"
import { generateRandomString } from "better-auth/crypto"
import type { NextRequest } from "next/server"
import { NextResponse } from "next/server"
import { z } from "zod"
import { verifyBridgeSecret } from "../verify-secret"

/**
 * Mints a Better Auth session for an EXISTING ChatbotX user, wrapped as a
 * one-time-token, without needing that user's password or a pre-existing
 * browser session.
 *
 * This deliberately does not call Better Auth's own
 * `/api/auth/one-time-token/generate` endpoint: that endpoint mints a token
 * for whatever session is already attached to the *calling* request, with no
 * way to pass an arbitrary target userId — unusable for a stateless
 * server-to-server bridge. Instead this writes the exact two rows that
 * endpoint would have written, so the existing, unmodified
 * `/api/auth/one-time-token/verify` endpoint (and the `oneTimeTokenClient()`
 * already wired in packages/auth/src/client.ts) can redeem it normally:
 *
 *   Session      { token: <sessionToken>, userId, expiresAt }
 *   Verification { identifier: "one-time-token:<otpToken>", value: <sessionToken>, expiresAt: now+3min }
 *
 * The caller (UpUnity's backend) gets back only `otpToken` and redirects the
 * user's browser to a ChatbotX page that calls
 * `authClient.oneTimeToken.verify({ token: otpToken })` — see
 * apps/builder/src/app/sso-landing/page.tsx.
 */

const requestSchema = z.object({
  chatbotxUserId: z.string().min(1),
  /** Session lifetime once redeemed. Defaults to 12h — long enough for a
   * work session, short enough that a stale UpUnity-side mapping doesn't
   * hand out a near-permanent ChatbotX session. */
  sessionTtlSeconds: z
    .number()
    .int()
    .positive()
    .max(60 * 60 * 24 * 7)
    .default(60 * 60 * 12),
})

const OTP_TTL_MS = 3 * 60 * 1000 // matches better-auth's oneTimeToken plugin default
const TOKEN_LENGTH = 32

export async function POST(request: NextRequest) {
  const rejected = verifyBridgeSecret(request)
  if (rejected) {
    return rejected
  }

  const parsed = requestSchema.safeParse(await request.json().catch(() => null))
  if (!parsed.success) {
    return NextResponse.json(
      { error: "invalid_request", issues: parsed.error.issues },
      { status: 400 },
    )
  }
  const { chatbotxUserId, sessionTtlSeconds } = parsed.data

  const user = await db.query.userModel.findFirst({
    where: { id: chatbotxUserId },
    columns: { id: true },
  })
  if (!user) {
    return NextResponse.json({ error: "user_not_found" }, { status: 404 })
  }

  const now = Date.now()
  const sessionToken = generateRandomString(TOKEN_LENGTH, "a-z", "A-Z", "0-9")
  const otpToken = generateRandomString(TOKEN_LENGTH, "a-z", "A-Z", "0-9")

  await db.transaction(async (tx) => {
    await tx.insert(sessionModel).values({
      token: sessionToken,
      userId: chatbotxUserId,
      expiresAt: new Date(now + sessionTtlSeconds * 1000),
    })

    await tx.insert(verificationModel).values({
      identifier: `one-time-token:${otpToken}`,
      value: sessionToken,
      expiresAt: new Date(now + OTP_TTL_MS),
    })
  })

  return NextResponse.json({
    token: otpToken,
    expiresInSeconds: OTP_TTL_MS / 1000,
  })
}
