import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import { postFrame } from "./broadcastRpc.js"
import * as Wire from "./multiTabWire.js"
import * as platform from "./platform.js"

export interface LeadershipOptions {
  readonly name: string
  readonly tabId: Wire.TabId
  readonly connection: platform.TabChannelConnection
  readonly retryDelay: Duration.Input
  readonly whileLeader: (epoch: Wire.Epoch) => Effect.Effect<boolean, never, Scope.Scope>
}

export interface Leadership {
  readonly requestSteal: Effect.Effect<void>
}

export const lockName = (name: string): string => `@lucas-barake/effect-local-browser:${name}:leader`

export const epochKey = (name: string): string => `@lucas-barake/effect-local-browser:${name}:epoch`

export const makeLeadership = Effect.fnUntraced(function*(
  options: LeadershipOptions
) {
  const locks = yield* platform.WebLocks
  const epochs = yield* platform.EpochStore
  const retryMillis = Duration.toMillis(options.retryDelay)
  const name = lockName(options.name)
  const key = epochKey(options.name)
  let steal = yield* Deferred.make<void>()

  const lead = Effect.scoped(Effect.gen(function*() {
    const stealNow = yield* Deferred.isDone(steal)
    let hold: platform.WebLockHold
    if (stealNow) {
      hold = yield* locks.acquire(name, { steal: true })
    } else {
      hold = yield* Effect.raceFirst(
        locks.acquire(name),
        Deferred.await(steal).pipe(
          Effect.andThen(locks.acquire(name, { steal: true }))
        )
      )
    }
    steal = yield* Deferred.make<void>()
    const epoch = Wire.Epoch.make(yield* epochs.bump(key))
    yield* postFrame(options.connection, Wire.Elected.make({ epoch, leaderId: options.tabId }))
    yield* options.whileLeader(epoch)
    yield* hold.lost
  }))

  yield* lead.pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("leadership attempt ended abnormally").pipe(
        Effect.annotateLogs({ cause: String(cause) }),
        Effect.andThen(Effect.sleep(retryMillis))
      )
    ),
    Effect.forever,
    Effect.forkScoped
  )

  const leadership: Leadership = {
    requestSteal: Effect.suspend(() => Deferred.succeed(steal, undefined))
  }
  return leadership
})
