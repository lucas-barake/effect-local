import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import type { Service } from "../ProtocolSession.js"

export const run = <A,>(
  self: Service,
  execute: (version: Protocol.ProtocolVersion) => Effect.Effect<A, ReplicaError.ReplicaError>
) =>
  Effect.gen(function*() {
    const version = yield* self.version
    const first = yield* execute(version).pipe(Effect.result)
    if (first._tag === "Success") return first.success
    if (first.failure._tag !== "ProtocolVersionRejected") return yield* first.failure
    return yield* execute(yield* self.rejected(version))
  })

export const runStream = <A,>(
  self: Service,
  execute: (version: Protocol.ProtocolVersion) => Stream.Stream<A, ReplicaError.ReplicaError>
) =>
  Stream.unwrap(self.version.pipe(
    Effect.map((version) =>
      execute(version).pipe(
        Stream.catchTag(
          "ProtocolVersionRejected",
          () => Stream.unwrap(self.rejected(version).pipe(Effect.map(execute)))
        )
      )
    )
  ))
