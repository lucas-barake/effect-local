import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Redacted from "effect/Redacted"
import type * as PeerRpcError from "./PeerRpcError.js"

export class PeerCredentials extends Context.Service<PeerCredentials, {
  readonly get: Effect.Effect<Redacted.Redacted<string>, PeerRpcError.AuthenticationFailure>
}>()("@lucas-barake/effect-local-rpc/PeerCredentials") {}
