import { timingSafeEqual } from "node:crypto"
import { keys } from "@chatbotx.io/business"
import type { NextRequest } from "next/server"
import { NextResponse } from "next/server"

/**
 * Shared guard for every /api/internal/upunity-bridge/* route: requires
 * `Authorization: Bearer <UPUNITY_BRIDGE_SECRET>` and constant-time-compares
 * it against the configured secret. Returns a ready-to-send NextResponse when
 * the request should be rejected, or `null` when it's authorized to proceed.
 *
 * The bridge is hard-disabled (503, not "any secret works") until an operator
 * explicitly sets UPUNITY_BRIDGE_SECRET — an unset/empty configured secret
 * must never be treated as "no auth required".
 */
export function verifyBridgeSecret(request: NextRequest): NextResponse | null {
  const configured = keys().UPUNITY_BRIDGE_SECRET
  if (!configured) {
    return NextResponse.json(
      { error: "upunity_bridge_disabled" },
      { status: 503 },
    )
  }

  const header = request.headers.get("authorization") ?? ""
  const provided = header.startsWith("Bearer ") ? header.slice(7) : ""

  const a = Buffer.from(provided)
  const b = Buffer.from(configured)
  const authorized =
    a.length === b.length && provided.length > 0 && timingSafeEqual(a, b)

  if (!authorized) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 })
  }

  return null
}
