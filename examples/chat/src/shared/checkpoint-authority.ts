import type * as CheckpointAuthority from "@lucas-barake/effect-local-sql/CheckpointAuthority"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"

/**
 * Without an authority a replica neither ships nor accepts compact checkpoints, so a peer that
 * falls far enough behind that its remaining history no longer fits one sync message can never
 * catch up. Checkpoint bytes carry a document's whole state, so the receiver has to be told by
 * something other than the sender that they are legitimate; that is what these four operations are.
 *
 * This demo signs with one hardcoded HMAC secret, which is the same shortcut the identities take:
 * anyone who can read the bundle can mint a checkpoint for any conversation. A real deployment
 * signs with a key the application server holds and verifies against its own trust policy.
 */
const demoSecret = "effect-local-chat-demo-checkpoint-authority"

const rejected = (documentId: Identity.DocumentId, reason: string) =>
  new ReplicaError.ReplicaError({ reason: new ReplicaError.CheckpointRejected({ documentId, reason }) })

const importKey = Effect.promise(() =>
  globalThis.crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(demoSecret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  )
)

export const demoAuthority: Effect.Effect<CheckpointAuthority.Implementation> = Effect.map(
  importKey,
  (key) => {
    // `CheckpointAuthority.layer` canonicalizes every heads collection before it calls the
    // implementation, so both peers sign the identical string for the identical claims.
    const claimBytes = (claims: unknown) => new TextEncoder().encode(Canonical.stringify(claims))

    const sign = (claims: unknown) =>
      Effect.promise(() => globalThis.crypto.subtle.sign("HMAC", key, claimBytes(claims))).pipe(
        Effect.map((signature) => Option.some(new Uint8Array(signature) as CheckpointAuthority.AuthorizationToken))
      )

    const verify = (documentId: Identity.DocumentId, claims: unknown, token: Uint8Array, subject: string) =>
      Effect.promise(() => globalThis.crypto.subtle.verify("HMAC", key, new Uint8Array(token), claimBytes(claims)))
        .pipe(
          Effect.flatMap((valid) => valid ? Effect.void : rejected(documentId, `${subject} signature does not verify`))
        )

    return {
      signManifest: sign,
      verifyManifest: (claims, token) => verify(claims.documentId, claims, token, "Checkpoint manifest"),
      signTransition: sign,
      verifyTransition: (claims, token) => verify(claims.documentId, claims, token, "Lineage transition")
    }
  }
)
