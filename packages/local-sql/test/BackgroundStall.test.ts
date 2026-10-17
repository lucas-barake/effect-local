import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  type Constructor,
  constructors,
  describeExit,
  emptyPage,
  eventually,
  idleRemote,
  installView,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000e101")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000e101")

const acceptedAhead = Effect.fnUntraced(function*(constructor: Constructor, coveredFromTurn: number) {
  const services = yield* BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "8 seconds"
  })
  yield* BackgroundReplica.seedPending(services, [spaceId])
  const turns: Array<number> = []
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    transportGeneration: Effect.map(Clock.currentTimeMillis, (now) => {
      turns.push(now)
      return 0
    }),
    submitBatch: (request) =>
      Effect.succeed(Protocol.SubmitBatchResult.make({
        receipts: request.envelopes.map((envelope) =>
          Protocol.AcceptedReceipt.make({
            ...envelope,
            serverSequence: Identity.ServerSequence.make(5),
            result: Domain.todo(envelope.mutationId, "accepted")
          })
        )
      })),
    pull: (request) =>
      emptyPage(services.crypto, request).pipe(
        Effect.map((page) => {
          let serverSequence = 0
          if (turns.length >= coveredFromTurn) serverSequence = 5
          return Protocol.PullPage.make({ ...page, serverSequence: Identity.ServerSequence.make(serverSequence) })
        })
      )
  }))
  return { services, turns, space: yield* replica.space(spaceId) }
})

describe("a background turn that ends before the server view covers what the server accepted", () => {
  it.effect.each(constructors)(
    "is run again after the retry delay and drains the space with %s",
    Effect.fnUntraced(function*(constructor) {
      const { services, space, turns } = yield* acceptedAhead(constructor, 2)

      const drained = yield* eventually(services, space, (status) => status.pending === 0)

      assert.isTrue(Option.isSome(drained), "the accepted mutation settled")
      assert.deepStrictEqual(turns.map((time) => time - turns[0]), [0, 1000])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "is retried with a growing and capped delay while it keeps making no progress with %s",
    Effect.fnUntraced(function*(constructor) {
      const { services, space, turns } = yield* acceptedAhead(constructor, 7)

      const drained = yield* eventually(services, space, (status) => status.pending === 0)

      assert.isTrue(Option.isSome(drained), "the accepted mutation settled")
      assert.deepStrictEqual(turns.map((time) => time - turns[0]), [0, 1000, 3000, 7000, 15000, 23000, 31000])
    }, VirtualTime.scoped)
  )
})

describe("a background turn that settled part of what the server accepted", () => {
  it.effect.each(constructors)(
    "is followed by the next turn at once and backs off only when a turn settles nothing with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [spaceId],
        maximumActiveSpaces: 4,
        foregroundActiveSpaces: 2,
        retryDelay: "1 second",
        maximumRetryDelay: "8 seconds"
      })
      const seedScope = yield* Scope.make()
      const seed = yield* services.start(idleRemote).pipe(Scope.provide(seedScope))
      const seeded = yield* seed.space(spaceId)
      yield* seeded.mutate(Domain.PutTodo, Domain.todo("one"))
      yield* seeded.mutate(Domain.PutTodo, Domain.todo("two"))
      yield* seeded.deactivate
      yield* Scope.close(seedScope, Exit.void)
      yield* installView(services)
      const accepted = new Map<string, number>()
      const pulls: Array<number> = []
      yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: (request) =>
          Effect.succeed(Protocol.SubmitBatchResult.make({
            receipts: request.envelopes.map((envelope) => {
              const sequence = accepted.get(envelope.mutationId) ?? accepted.size + 5
              accepted.set(envelope.mutationId, sequence)
              return Protocol.AcceptedReceipt.make({
                ...envelope,
                serverSequence: Identity.ServerSequence.make(sequence),
                result: Domain.todo(envelope.mutationId, "accepted")
              })
            })
          })),
        pull: (request) =>
          Clock.currentTimeMillis.pipe(
            Effect.tap((now) => Effect.sync(() => pulls.push(now))),
            Effect.andThen(emptyPage(services.crypto, request)),
            Effect.map((page) => Protocol.PullPage.make({ ...page, serverSequence: Identity.ServerSequence.make(5) }))
          )
      }))

      yield* VirtualTime.quiet("3500 millis")
      const turnsAt = (time: number) => pulls.filter((pulled) => pulled - pulls[0] === time).length / 2

      assert.deepStrictEqual(Array.from(new Set(pulls.map((time) => time - pulls[0]))), [0, 1000, 3000])
      assert.strictEqual(turnsAt(0), 2, "the turn that made progress was followed by another at once")
    }, VirtualTime.scoped)
  )
})

describe("a foreground sync that ends before the server view covers what the server accepted", () => {
  it.effect.each(constructors)(
    "is run again with a growing delay until the mutation settles with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [spaceId],
        maximumActiveSpaces: 4,
        foregroundActiveSpaces: 2,
        retryDelay: "1 second",
        maximumRetryDelay: "8 seconds"
      })
      let covered = false
      const pulls: Array<number> = []
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: (request) =>
          Effect.succeed(Protocol.SubmitBatchResult.make({
            receipts: request.envelopes.map((envelope) =>
              Protocol.AcceptedReceipt.make({
                ...envelope,
                serverSequence: Identity.ServerSequence.make(5),
                result: Domain.todo(envelope.mutationId, "accepted")
              })
            )
          })),
        pull: (request) =>
          Clock.currentTimeMillis.pipe(
            Effect.tap((now) => Effect.sync(() => pulls.push(now))),
            Effect.andThen(emptyPage(services.crypto, request)),
            Effect.map((page) => {
              let serverSequence = 0
              if (covered) serverSequence = 5
              return Protocol.PullPage.make({ ...page, serverSequence: Identity.ServerSequence.make(serverSequence) })
            })
          )
      }))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending")).pipe(VirtualTime.advanceUntil)
      yield* VirtualTime.quiet("20 seconds")
      const retriedAt = Array.from(new Set(pulls.map((time) => time - pulls[0])))

      covered = true
      const drained = yield* eventually(services, space, (status) => status.pending === 0)

      assert.deepStrictEqual(retriedAt, [0, 1000, 3000, 7000, 15000])
      assert.isTrue(Option.isSome(drained), "the accepted mutation settled once the view covered it")
    }, VirtualTime.scoped)
  )
})

const acceptedAheadInTheForeground = Effect.fnUntraced(function*(constructor: Constructor) {
  const services = yield* BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })
  const accepted = new Map<string, number>()
  const view = { coveredThrough: 0 }
  const pulls: Array<number> = []
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: (request) =>
      Effect.succeed(Protocol.SubmitBatchResult.make({
        receipts: request.envelopes.map((envelope) => {
          const sequence = accepted.get(envelope.mutationId) ?? accepted.size + 5
          accepted.set(envelope.mutationId, sequence)
          return Protocol.AcceptedReceipt.make({
            ...envelope,
            serverSequence: Identity.ServerSequence.make(sequence),
            result: Domain.todo(envelope.mutationId, "accepted")
          })
        })
      })),
    pull: (request) =>
      Clock.currentTimeMillis.pipe(
        Effect.tap((now) => Effect.sync(() => pulls.push(now))),
        Effect.andThen(emptyPage(services.crypto, request)),
        Effect.map((page) =>
          Protocol.PullPage.make({ ...page, serverSequence: Identity.ServerSequence.make(view.coveredThrough) })
        )
      )
  }))
  yield* installView(services)
  const space = yield* replica.space(spaceId)
  const write = (id: string) => space.mutate(Domain.PutTodo, Domain.todo(id)).pipe(VirtualTime.advanceUntil)
  const pulledAt = () => Array.from(new Set(pulls.map((time) => time - pulls[0])))
  return { view, write, pulledAt }
})

describe("the retry of a foreground sync that left accepted work pending", () => {
  it.effect.each(constructors)(
    "starts over with the first delay once a sync made progress with %s",
    Effect.fnUntraced(function*(constructor) {
      const { pulledAt, view, write } = yield* acceptedAheadInTheForeground(constructor)
      yield* write("first")
      yield* write("second")
      yield* VirtualTime.quiet("3500 millis")
      const beforeProgress = pulledAt()

      view.coveredThrough = 5
      yield* VirtualTime.quiet("7 seconds")

      assert.deepStrictEqual(beforeProgress, [0, 1000, 3000])
      assert.deepStrictEqual(pulledAt(), [0, 1000, 3000, 7000, 8000, 10000])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "is dropped when a later sync drained the space with %s",
    Effect.fnUntraced(function*(constructor) {
      const { pulledAt, view, write } = yield* acceptedAheadInTheForeground(constructor)
      yield* write("first")
      yield* VirtualTime.quiet("500 millis")

      view.coveredThrough = 6
      yield* write("second")
      yield* VirtualTime.quiet("1 minute")

      assert.deepStrictEqual(pulledAt(), [0, 500])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "keeps its time when another sync stalls before it with %s",
    Effect.fnUntraced(function*(constructor) {
      const { pulledAt, write } = yield* acceptedAheadInTheForeground(constructor)
      yield* write("first")
      yield* VirtualTime.quiet("500 millis")

      yield* write("second")
      yield* VirtualTime.quiet("3 seconds")

      assert.deepStrictEqual(pulledAt(), [0, 500, 1000, 3000])
    }, VirtualTime.scoped)
  )
})

const failingServer = Effect.fnUntraced(function*(constructor: Constructor, seeded: boolean) {
  const services = yield* BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })
  if (seeded) yield* BackgroundReplica.seedPending(services, [spaceId])
  const server = { failures: 0 }
  const pulls: Array<number> = []
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: acceptSubmission,
    pull: (request) => {
      const answered = Effect.suspend((): Effect.Effect<void, ReplicaError.ServerUnavailable> => {
        if (server.failures === 0) return Effect.void
        server.failures -= 1
        return Effect.fail(new ReplicaError.ServerUnavailable())
      })
      return Clock.currentTimeMillis.pipe(
        Effect.tap((now) => Effect.sync(() => pulls.push(now))),
        Effect.andThen(answered),
        Effect.andThen(emptyPage(services.crypto, request))
      )
    }
  }))
  if (!seeded) yield* installView(services)
  const space = yield* replica.space(spaceId)
  return { services, replica, server, pulls, space }
})

describe("the retry position a space keeps for its server calls", () => {
  it.effect.each(constructors)(
    "starts over after a sync succeeded with %s",
    Effect.fnUntraced(function*(constructor) {
      const { pulls, server, space } = yield* failingServer(constructor, false)
      server.failures = 3
      yield* space.mutate(Domain.PutTodo, Domain.todo("first")).pipe(VirtualTime.advanceUntil)
      yield* VirtualTime.quiet("10 seconds")
      const before = pulls.length

      server.failures = 1
      yield* space.mutate(Domain.PutTodo, Domain.todo("second")).pipe(VirtualTime.advanceUntil)
      yield* VirtualTime.quiet("5 seconds")
      const later = Array.from(new Set(pulls.slice(before).map((time) => time - pulls[before])))

      assert.deepStrictEqual(later, [0, 1000], "the call after one new failure waited the first delay")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "starts over when the space is made foreground with %s",
    Effect.fnUntraced(function*(constructor) {
      const { pulls, server, space } = yield* failingServer(constructor, true)
      server.failures = 3
      yield* VirtualTime.quiet("3500 millis")
      const inTheBackground = Array.from(new Set(pulls.map((time) => time - pulls[0])))
      const before = pulls.length

      yield* VirtualTime.advanceUntil(space.activate)
      yield* VirtualTime.quiet("500 millis")

      assert.deepStrictEqual(inTheBackground, [0, 1000, 3000])
      assert.strictEqual(pulls[before] - pulls[0], 3500, "the foreground sync did not wait for the background position")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "holds back the turn that follows a turn retired just as its call failed with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [spaceId],
        maximumActiveSpaces: 4,
        foregroundActiveSpaces: 2,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      yield* BackgroundReplica.seedPending(services, [spaceId])
      const calling = yield* Deferred.make<void>()
      const failing = yield* Deferred.make<void, ReplicaError.ServerUnavailable>()
      const pulls: Array<number> = []
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) =>
          Clock.currentTimeMillis.pipe(
            Effect.flatMap((now) => {
              pulls.push(now)
              if (pulls.length > 1) return Effect.void
              return Effect.andThen(Deferred.succeed(calling, undefined), Deferred.await(failing))
            }),
            Effect.andThen(emptyPage(services.crypto, request))
          )
      }))
      const space = yield* replica.space(spaceId)
      yield* VirtualTime.advanceUntil(Deferred.await(calling))

      const reporting = yield* services.holdStatement("effect_local_client_pending_data", true)
      yield* Deferred.fail(failing, new ReplicaError.ServerUnavailable())
      yield* VirtualTime.advanceUntil(reporting.entered)
      yield* VirtualTime.advanceUntil(space.deactivate)
      yield* reporting.release
      yield* VirtualTime.quiet("5 seconds")

      assert.strictEqual(pulls[1] - pulls[0], 1000, "the next call waited the first delay")
    }, VirtualTime.scoped)
  )
})

describe("a foreground operation that is turned away while its space is being left", () => {
  it.effect.each(constructors)(
    "does not start a background turn when the leave fails with %s",
    Effect.fnUntraced(function*(constructor) {
      const { pulls, replica, services, space } = yield* failingServer(constructor, false)
      yield* VirtualTime.advanceUntil(space.activate)
      yield* VirtualTime.quiet("1 second")
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces")
      const leaving = yield* replica.leave(spaceId).pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(removal.entered)

      const turnedAway = yield* within(space.get(Domain.Todo, "x"))
      const before = pulls.length
      yield* removal.release
      const left = yield* VirtualTime.advanceUntil(Fiber.join(leaving))
      yield* VirtualTime.quiet("1 minute")

      assert.strictEqual(describeExit(turnedAway), "failed")
      assert.isTrue(Exit.isFailure(left), "the leave failed at its delete")
      assert.strictEqual(pulls.length, before, "no sync was started for the space")
    }, VirtualTime.scoped)
  )
})
