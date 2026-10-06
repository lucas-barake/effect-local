import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
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

const first = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000e301")
const second = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000e302")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000e301")

const quiet = (duration: "1 millis" | "1 minute" | "1 hour") =>
  VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption(duration))

const acceptedAt = (request: Parameters<BackgroundReplica.Remote["submitBatch"]>[0], sequence: number) =>
  Protocol.SubmitBatchResult.make({
    receipts: request.envelopes.map((envelope) =>
      Protocol.AcceptedReceipt.make({
        ...envelope,
        serverSequence: Identity.ServerSequence.make(sequence),
        result: Domain.todo(envelope.mutationId, "accepted")
      })
    )
  })

const onePlace = (constructor: Constructor) =>
  BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [first, second],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 1,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })

const keptPages = constructors.flatMap((constructor) => [false, true].map((hasMore) => ({ constructor, hasMore })))

describe("an answer kept from a server call that outlived its turn", () => {
  it.effect.each(constructors)(
    "is gone when its space is activated an hour later, and the server is asked again with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* onePlace(constructor)
      const wakes = yield* Queue.unbounded<Protocol.Wake>()
      const held = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let holdNext = false
      let asked = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        watch: (request) => {
          if (request.spaceId !== first) return Stream.never
          return Stream.fromQueue(wakes)
        },
        pull: (request) => {
          const page = emptyPage(services.crypto, request)
          if (request.spaceId !== first) return page
          asked += 1
          if (!holdNext) return page
          holdNext = false
          return Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.andThen(page))
        }
      }))
      yield* installView(services)
      const a = yield* replica.space(first)
      const b = yield* replica.space(second)
      yield* VirtualTime.advanceUntil(a.activate)
      yield* eventually(services, a, (status) => status._tag === "Online")
      yield* quiet("1 minute")
      holdNext = true
      yield* Queue.offer(wakes, Protocol.Wake.make({ spaceId: first }))
      yield* VirtualTime.advanceUntil(Deferred.await(held))
      const evicted = yield* within(b.get(Domain.Todo, "other"))
      yield* Deferred.succeed(release, undefined)
      yield* quiet("1 hour")
      const askedBefore = asked

      yield* VirtualTime.advanceUntil(a.activate)
      const back = yield* eventually(services, a, (status) => status._tag === "Online")
      yield* quiet("1 minute")

      assert.strictEqual(describeExit(evicted), "succeeded")
      assert.isTrue(Option.isSome(back), "the space came online again")
      assert.isAbove(asked, askedBefore, "the server was pulled after the space came back")
    }, VirtualTime.scoped)
  )

  it.effect.each(keptPages)(
    "advances the next pass, which then asks the server itself before it reports success ($constructor, more pages: $hasMore)",
    Effect.fnUntraced(function*(row) {
      const services = yield* onePlace(row.constructor)
      const wakes = yield* Queue.unbounded<Protocol.Wake>()
      const held = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const backgroundMayStart = yield* Deferred.make<void>()
      let holdNext = false
      let holdBackground = false
      let covered = false
      let otherClientWrote = false
      const pulls: Array<{ readonly afterTheWrite: boolean }> = []
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        transportGeneration: Effect.suspend(() => {
          if (!holdBackground) return Effect.succeed(0)
          return Effect.as(Deferred.await(backgroundMayStart), 0)
        }),
        submitBatch: (request) => Effect.succeed(acceptedAt(request, 5)),
        watch: (request) => {
          if (request.spaceId !== first) return Stream.never
          return Stream.fromQueue(wakes)
        },
        pull: (request) => {
          if (request.spaceId !== first) return emptyPage(services.crypto, request)
          pulls.push({ afterTheWrite: otherClientWrote })
          let serverSequence = 0
          if (covered) serverSequence = 5
          const kept = holdNext
          holdNext = false
          const page = emptyPage(services.crypto, request).pipe(
            Effect.map((answer) =>
              Protocol.PullPage.make({
                ...answer,
                serverSequence: Identity.ServerSequence.make(serverSequence),
                hasMore: kept && row.hasMore
              })
            )
          )
          if (!kept && !holdBackground) return page
          return Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.andThen(page))
        }
      }))
      yield* installView(services)
      const a = yield* replica.space(first)
      const b = yield* replica.space(second)
      yield* a.mutate(Domain.PutTodo, Domain.todo("pending")).pipe(VirtualTime.advanceUntil)
      yield* eventually(services, a, (status) => status._tag === "Online")
      yield* quiet("1 minute")
      covered = true
      holdNext = true
      yield* Queue.offer(wakes, Protocol.Wake.make({ spaceId: first }))
      yield* VirtualTime.advanceUntil(Deferred.await(held))
      holdBackground = true
      const evicted = yield* within(b.get(Domain.Todo, "other"))
      const pulledBefore = pulls.length
      otherClientWrote = true
      yield* Deferred.succeed(release, undefined)
      yield* quiet("1 millis")

      yield* Deferred.succeed(backgroundMayStart, undefined)
      const drained = yield* eventually(services, a, (status) => status.pending === 0)
      yield* quiet("1 minute")

      assert.strictEqual(describeExit(evicted), "succeeded")
      assert.isTrue(Option.isSome(drained), "the kept answer settled the accepted mutation")
      assert.isAtLeast(pulls.length - pulledBefore, 1, "the pass pulled itself after the kept answer")
      assert.isTrue(pulls[pulls.length - 1].afterTheWrite, "its last pull was sent after the other client wrote")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "is dropped when the server rejects the credential, so the submit is sent again with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* onePlace(constructor)
      const held = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const backgroundMayStart = yield* Deferred.make<void>()
      const credentialChanged = yield* Deferred.make<void>()
      let rejectPulls = false
      let holdBackground = false
      let submits = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        transportGeneration: Effect.suspend(() => {
          if (!holdBackground) return Effect.succeed(0)
          return Effect.as(Deferred.await(backgroundMayStart), 0)
        }),
        waitForCredentialChange: () => Deferred.await(credentialChanged),
        submitBatch: (request) => {
          submits += 1
          const accepted = acceptSubmission(request)
          if (submits > 1) return accepted
          return Deferred.succeed(held, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(accepted)
          )
        },
        pull: (request) => {
          if (request.spaceId !== first || !holdBackground) return emptyPage(services.crypto, request)
          const afterTheGate = (): Effect.Effect<Protocol.PullResult, ReplicaError.ReplicaError> => {
            if (!rejectPulls) return emptyPage(services.crypto, request)
            return Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
          }
          return Effect.flatMap(Deferred.await(backgroundMayStart), afterTheGate)
        }
      }))
      yield* installView(services)
      const a = yield* replica.space(first)
      const b = yield* replica.space(second)
      yield* a.mutate(Domain.PutTodo, Domain.todo("pending")).pipe(VirtualTime.advanceUntil)
      yield* VirtualTime.advanceUntil(Deferred.await(held))
      holdBackground = true
      const evicted = yield* within(b.get(Domain.Todo, "other"))
      yield* Deferred.succeed(release, undefined)
      yield* quiet("1 millis")
      rejectPulls = true
      yield* Deferred.succeed(backgroundMayStart, undefined)
      const rejected = yield* eventually(services, a, (status) => status._tag === "NeedsAuthentication")

      rejectPulls = false
      yield* Deferred.succeed(credentialChanged, undefined)
      const drained = yield* eventually(services, a, (status) => status.pending === 0)

      assert.strictEqual(describeExit(evicted), "succeeded")
      assert.isTrue(Option.isSome(rejected), "the rejection was reported")
      assert.isTrue(Option.isSome(drained), "the mutation drained under the new credential")
      assert.strictEqual(submits, 2, "the submit was sent again and not answered from before the rejection")
    }, VirtualTime.scoped)
  )
})
