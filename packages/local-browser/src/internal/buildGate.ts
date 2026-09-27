import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as BuildIdentity from "./buildIdentity.js"
import type * as lockNames from "./lockNames.js"
import * as LosslessQueue from "./losslessQueue.js"
import type * as platform from "./platform.js"

export interface Options {
  readonly host: string
  readonly build: BuildIdentity.BuildIdentity
  readonly names: lockNames.LockNames
  readonly locks: platform.WebLocksService
  readonly channels: platform.TabChannelService
}

export interface BuildGate {
  readonly superseded: Deferred.Deferred<never, ReplicaError.BuildSuperseded>
  readonly check: Effect.Effect<boolean>
}

const Counter = Schema.FiniteFromString.check(Schema.isInt())

const PresenceParts = Schema.Tuple([Counter, Counter, Schema.String, Schema.String])

const decodePresence = Schema.decodeUnknownEffect(PresenceParts)

const outranks = (left: lockNames.Presence, right: lockNames.Presence): boolean => {
  if (left.version !== right.version) return left.version > right.version
  if (left.sequence !== right.sequence) return left.sequence > right.sequence
  return left.host > right.host
}

export const make = Effect.fnUntraced(function*(options: Options) {
  const gateScope = yield* Effect.scope
  const names = options.names
  const channel = yield* options.channels.open(names.presenceChannel)
  const nudges = yield* channel.messages

  const presences = options.locks.held.pipe(
    Effect.flatMap(Effect.forEach((name) => {
      if (!name.startsWith(names.presencePrefix)) return Effect.succeedNone
      return decodePresence(name.slice(names.presencePrefix.length).split(":")).pipe(
        Effect.map(([sequence, version, fingerprint, host]) =>
          Option.some<lockNames.Presence>({ sequence, version, fingerprint, host })
        ),
        Effect.catchTag("SchemaError", () => Effect.succeedNone)
      )
    })),
    Effect.map((entries) => entries.flatMap(Option.toArray))
  )

  let sequence = 0
  for (const presence of yield* presences) sequence = Math.max(sequence, presence.sequence)
  const self: lockNames.Presence = {
    sequence: sequence + 1,
    version: options.build.version,
    fingerprint: options.build.fingerprint,
    host: options.host
  }
  yield* options.locks.acquire(names.presence(self))

  const superseded = yield* Deferred.make<never, ReplicaError.BuildSuperseded>()

  const evaluate = presences.pipe(
    Effect.flatMap((entries) => {
      let top: lockNames.Presence | undefined
      for (const entry of entries) {
        if (entry.fingerprint === self.fingerprint || !outranks(entry, self)) continue
        if (top === undefined || outranks(entry, top)) top = entry
      }
      if (top === undefined) return Effect.succeed(false)
      return Deferred.fail(
        superseded,
        new ReplicaError.BuildSuperseded({ version: self.version, supersedingVersion: top.version })
      ).pipe(Effect.as(true))
    })
  )

  const check = Deferred.isDone(superseded).pipe(
    Effect.flatMap((done) => {
      if (done) return Effect.succeed(true)
      return evaluate
    })
  )

  if (!(yield* check)) {
    yield* channel.post(options.host)
    yield* LosslessQueue.take(nudges).pipe(
      Effect.andThen(check),
      Effect.repeat({ until: (current) => current }),
      Effect.forkIn(gateScope)
    )
  }

  const gate: BuildGate = { superseded, check }
  return gate
})
