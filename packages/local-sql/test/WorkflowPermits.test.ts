import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  constructors,
  describeExit,
  emptyPage,
  eventually,
  idleRemote,
  installView,
  isOnlineDrained,
  makeCapacityProbe,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const evicted = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f801")
const current = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f802")
const probes = [3, 4, 5, 6].map((index) => Identity.SpaceId.make(`spc_00000000-0000-4000-8000-00000000f80${index}`))
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000f801")
const backgroundTurns = 3
const settings = (constructor: BackgroundReplica.Constructor) => ({
  constructor,
  clientId,
  initialSpaces: [evicted, current, ...probes],
  maximumActiveSpaces: 1 + backgroundTurns,
  foregroundActiveSpaces: 1,
  reconciliationConcurrency: 1 + backgroundTurns,
  retryDelay: "1 second",
  maximumRetryDelay: "1 minute"
} as const)

const quiet = VirtualTime.quiet("1 minute")

describe("a sync that is in flight when its space loses the only foreground place", () => {
  it.effect.each(constructors)(
    "does not keep the foreground reconciliation permit while it continues in the background with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services(settings(constructor))
      const probe = yield* makeCapacityProbe(probes)
      const calling = yield* Deferred.make<void>()
      const answered = yield* Deferred.make<void>()
      let held = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          const page = probe.held(request.spaceId, emptyPage(services.crypto, request))
          if (request.spaceId !== evicted) return page
          held += 1
          return Deferred.succeed(calling, undefined).pipe(
            Effect.andThen(Deferred.await(answered)),
            Effect.andThen(page)
          )
        }
      }))
      yield* installView(services)
      const a = yield* replica.space(evicted)
      const b = yield* replica.space(current)
      yield* a.mutate(Domain.PutTodo, Domain.todo("held")).pipe(VirtualTime.advanceUntil)
      yield* VirtualTime.advanceUntil(Deferred.await(calling))

      const written = yield* b.mutate(Domain.PutTodo, Domain.todo("after")).pipe(within)
      const synced = yield* eventually(services, b, isOnlineDrained)
      const pendingWhileHeld = (yield* a.status).pending
      yield* Deferred.succeed(answered, undefined)
      const drained = yield* eventually(services, a, (status) => status.pending === 0)
      yield* quiet
      const capacity = yield* probe.fill(services, replica, current)

      assert.strictEqual(describeExit(written), "succeeded")
      assert.isTrue(Option.isSome(synced), "the space that took the place synced while the other call was held")
      assert.strictEqual(pendingWhileHeld, 1, `the evicted space was still waiting on the server after ${held} calls`)
      assert.isTrue(Option.isSome(drained), "the evicted space drained in the background once the server answered")
      assert.strictEqual(yield* b.activation, "Active")
      assert.deepStrictEqual(
        capacity,
        { foregroundSynced: true, backgroundCallsAtOnce: backgroundTurns, drained: probes.length },
        "every permit, lease and place was free again"
      )
    }, VirtualTime.scoped)
  )
})

describe("a background sync that is in flight when its space is made foreground", () => {
  it.effect.each(constructors)(
    "does not keep a background reconciliation permit while it continues in the foreground with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services(settings(constructor))
      yield* BackgroundReplica.seedPending(services, [evicted, ...probes.slice(0, backgroundTurns)])
      const probe = yield* makeCapacityProbe(probes)
      const answered = yield* Deferred.make<void>()
      let waiting = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          const page = probe.held(request.spaceId, emptyPage(services.crypto, request))
          if (request.spaceId === evicted) return Effect.andThen(Deferred.await(answered), page)
          if (!probes.includes(request.spaceId)) return page
          waiting += 1
          return Deferred.await(answered).pipe(
            Effect.ensuring(Effect.sync(() => {
              waiting -= 1
            })),
            Effect.andThen(page)
          )
        }
      }))
      yield* quiet
      const promoted = yield* replica.space(evicted)

      const activated = yield* within(promoted.activate)
      yield* quiet
      const othersInFlight = waiting
      yield* Deferred.succeed(answered, undefined)
      const drained = yield* eventually(services, promoted, (status) => status.pending === 0)
      yield* quiet
      const capacity = yield* probe.fill(services, replica, current)

      assert.strictEqual(describeExit(activated), "succeeded")
      assert.strictEqual(
        othersInFlight,
        backgroundTurns,
        "background calls of other spaces while the promoted one waits"
      )
      assert.isTrue(Option.isSome(drained))
      assert.deepStrictEqual(
        capacity,
        { foregroundSynced: true, backgroundCallsAtOnce: backgroundTurns, drained: probes.length },
        "every permit, lease and place was free again"
      )
    }, VirtualTime.scoped)
  )
})
