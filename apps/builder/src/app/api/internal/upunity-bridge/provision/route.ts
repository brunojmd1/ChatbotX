import { workspaceMemberService, workspaceService } from "@chatbotx.io/business"
import { workspaceMemberRoles } from "@chatbotx.io/database/partials"
import type { NextRequest } from "next/server"
import { NextResponse } from "next/server"
import { z } from "zod"
import { findOrCreateUser } from "../find-or-create-user"
import { verifyBridgeSecret } from "../verify-secret"

/**
 * Server-to-server provisioning endpoint for the UpUnity <-> ChatbotX bridge.
 *
 * Two modes, matching the two moments UpUnity needs this:
 *
 * - `create_workspace`: called once, when an agency admin enables "UpChat"
 *   for a client company. Creates (or reuses) the ChatbotX user for the
 *   given owner email and, if that user doesn't already own a workspace,
 *   creates one — `workspaceService.create` makes the owner a full member
 *   internally, so no separate membership call is needed here.
 *
 * - `add_member`: called the first time an individual UpUnity user (who
 *   isn't the one who enabled the integration) clicks into the linked
 *   workspace. Creates (or reuses) the ChatbotX user and adds them as a
 *   member of the already-provisioned workspace if not already one.
 *
 * Both modes are idempotent: calling them again for the same
 * email/workspace returns the existing ids rather than erroring or
 * duplicating rows.
 */

const baseUserFields = {
  email: z.email(),
  name: z.string().min(1).max(200),
}

const requestSchema = z.discriminatedUnion("mode", [
  z.object({
    mode: z.literal("create_workspace"),
    ownerEmail: baseUserFields.email,
    ownerName: baseUserFields.name,
    workspaceName: z.string().min(1).max(200),
  }),
  z.object({
    mode: z.literal("add_member"),
    workspaceId: z.string().min(1),
    email: baseUserFields.email,
    name: baseUserFields.name,
  }),
])

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
  const input = parsed.data

  if (input.mode === "create_workspace") {
    const { userId, created: userCreated } = await findOrCreateUser({
      email: input.ownerEmail,
      name: input.ownerName,
    })

    const existingMemberships = await workspaceMemberService.listByUserId({
      userId,
    })
    const existingOwned = existingMemberships.find(
      (m) => m.role === workspaceMemberRoles.enum.owner,
    )
    if (existingOwned) {
      return NextResponse.json({
        chatbotxUserId: userId,
        chatbotxWorkspaceId: existingOwned.workspaceId,
        userCreated,
        workspaceCreated: false,
      })
    }

    const workspace = await workspaceService.create({
      data: { name: input.workspaceName },
      createdBy: userId,
    })

    return NextResponse.json({
      chatbotxUserId: userId,
      chatbotxWorkspaceId: workspace.id,
      userCreated,
      workspaceCreated: true,
    })
  }

  // mode === "add_member"
  const { userId, created: userCreated } = await findOrCreateUser({
    email: input.email,
    name: input.name,
  })

  const existingMembership =
    await workspaceMemberService.findByWorkspaceIdAndUserId({
      workspaceId: input.workspaceId,
      userId,
    })
  if (existingMembership) {
    return NextResponse.json({
      chatbotxUserId: userId,
      userCreated,
      memberCreated: false,
    })
  }

  await workspaceMemberService.create({
    data: {
      userId,
      workspaceId: input.workspaceId,
      role: workspaceMemberRoles.enum.agent,
      permissions: {
        superAdmin: false,
        analytics: false,
        flows: true,
        contacts: false,
        onlyAssignedContacts: true,
        emailAndPhone: false,
        broadcast: false,
        ecommerce: false,
      },
      notificationTypes: {
        notifyAdmin: false,
        newMessageToHuman: true,
        newOrder: false,
      },
      notificationChannels: {
        messenger: false,
        email: false,
        telegram: false,
        browser: true,
      },
    },
  })

  return NextResponse.json({
    chatbotxUserId: userId,
    userCreated,
    memberCreated: true,
  })
}
