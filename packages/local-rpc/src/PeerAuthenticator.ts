import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Redacted from "effect/Redacted"
import type { PeerPrincipal } from "./internal/peerPrincipal.js"
import type * as PeerRpcError from "./PeerRpcError.js"

export class PeerAuthenticator extends Context.Service<PeerAuthenticator, {
  readonly authenticate: (credential: Redacted.Redacted<string>) => Effect.Effect<{
    readonly principal: PeerPrincipal
    readonly validUntil: number
    readonly invalidated: Effect.Effect<void>
  }, PeerRpcError.AuthenticationFailure>
}>()("@lucas-barake/effect-local-rpc/PeerAuthenticator") {}
