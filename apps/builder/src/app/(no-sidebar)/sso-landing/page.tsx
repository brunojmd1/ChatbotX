"use client"

import { Loader2Icon, TriangleAlertIcon } from "lucide-react"
import { useRouter, useSearchParams } from "next/navigation"
import { useEffect, useState } from "react"
import { authClient } from "@/lib/auth/auth-client"
import { resolveSafeCallbackUrl } from "@/lib/safe-callback-url"

/**
 * SSO handoff landing page for the UpUnity <-> ChatbotX bridge.
 *
 * UpUnity's backend mints a one-time token via
 * /api/internal/upunity-bridge/sso-token and redirects the user's browser
 * here with `?token=...&redirect=/w/<workspaceId>/flows`. This page redeems
 * the token through Better Auth's own, unmodified
 * `authClient.oneTimeToken.verify` call — the resulting Set-Cookie on that
 * response is what actually logs the browser in — then forwards on to the
 * requested page.
 *
 * `redirect` is passed through `resolveSafeCallbackUrl` so a tampered value
 * can only ever send the user to a same-origin path, never an external site.
 */
export default function SsoLandingPage() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const [status, setStatus] = useState<"pending" | "error">("pending")

  useEffect(() => {
    const token = searchParams.get("token")
    if (!token) {
      setStatus("error")
      return
    }

    let cancelled = false

    authClient.oneTimeToken.verify({ token }).then(({ error }) => {
      if (cancelled) {
        return
      }
      if (error) {
        setStatus("error")
        return
      }

      const target = resolveSafeCallbackUrl(
        searchParams.get("redirect"),
        window.location.origin,
      )
      router.replace(target)
    })

    return () => {
      cancelled = true
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- runs once for this one-shot token
  }, [])

  return (
    <div className="flex min-h-screen flex-col items-center justify-center gap-3 text-muted-foreground">
      {status === "pending" ? (
        <>
          <Loader2Icon className="size-6 animate-spin" />
          <p>Conectando sua conta…</p>
        </>
      ) : (
        <>
          <TriangleAlertIcon className="size-6" />
          <p>
            Este link expirou ou já foi usado. Volte ao UpUnity e tente
            novamente.
          </p>
        </>
      )}
    </div>
  )
}
