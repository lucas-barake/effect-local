import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import type { Service } from "../ProtocolSession.js"
import { hasRemoteDefect } from "./remoteDefect.js"

export const run = Effect.fnUntraced(function*<A,>(
  self: Service,
  execute: (version: Protocol.ProtocolVersion) => Effect.Effect<A, ReplicaError.ReplicaError>
) {
  const version = yield* self.version
  return yield* execute(version).pipe(
    Effect.catchCause((cause) => {
      if (!hasRemoteDefect(cause)) return Effect.failCause(cause)
      return self.rejected(version).pipe(
        Effect.flatMap((renegotiated) => {
          if (renegotiated === version) return Effect.failCause(cause)
          return execute(renegotiated)
        })
      )
    }),
    Effect.catchTag(
      "ProtocolVersionRejected",
      () => self.rejected(version).pipe(Effect.flatMap(execute)),
      Effect.fail
    )
  )
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
