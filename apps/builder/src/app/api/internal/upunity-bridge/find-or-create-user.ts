import { db } from "@chatbotx.io/database/client"
import { ROOT_TENANT_ID, userModel } from "@chatbotx.io/database/schema"

/**
 * Find-or-create a ChatbotX `User` for a given email, scoped to the root
 * tenant (this is a single-tenant/community-edition bridge; white-label
 * tenants are out of scope here — see docs/tenancy.md).
 *
 * No credential `Account` row is created: users provisioned through the
 * UpUnity bridge only ever authenticate via the SSO one-time-token handoff
 * (see sso-token/route.ts), never via email/password sign-in, so there is no
 * password to store and nothing to email.
 *
 * `User_email_tenant_key` (email, tenantId) makes this racy-safe: a
 * concurrent call for the same email either sees the row from the `findFirst`
 * or loses the insert race and can retry the lookup — callers of this
 * function are internal/low-concurrency (one bridge call per UpUnity click),
 * so a simple check-then-insert is proportionate; no distributed lock.
 */
export async function findOrCreateUser(input: {
  email: string
  name: string
}): Promise<{ userId: string; created: boolean }> {
  const { email, name } = input

  const existing = await db.query.userModel.findFirst({
    where: { email, tenantId: ROOT_TENANT_ID },
    columns: { id: true },
  })
  if (existing) {
    return { userId: existing.id, created: false }
  }

  const [created] = await db
    .insert(userModel)
    .values({
      email,
      name,
      emailVerified: true,
      tenantId: ROOT_TENANT_ID,
    })
    .returning({ id: userModel.id })

  if (!created) {
    throw new Error("upunity-bridge: failed to create user")
  }

  return { userId: created.id, created: true }
}
