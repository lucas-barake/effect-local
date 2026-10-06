import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  captureErrors,
  type Constructor,
  constructors,
  emptyPage,
  eventually,
  idleRemote,
  installView,
  isOnlineDrained
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f601")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000f601")

const oneSpace = (constructor: Constructor) =>
  BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })

const fiveMinutes = VirtualTime.quiet("5 minutes")

describe("a credential generation that changes while a server call is in flight", () => {
  it.effect.each(constructors)(
    "does not hide a call that died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* oneSpace(constructor)
      const logs = captureErrors()
      let generation = 0
      let pulls = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        credentialGeneration: Effect.sync(() => generation),
        submitBatch: acceptSubmission,
        pull: (request) => {
          pulls += 1
          if (pulls > 1) return emptyPage(services.crypto, request)
          generation += 1
          return Effect.die("undecodable response")
        }
      })).pipe(Effect.provide(logs.layerLogs))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending")).pipe(VirtualTime.advanceUntil)

      const reported = yield* eventually(
        services,
        space,
        (status) => status._tag === "Failed" && status.message === "UnexpectedFailure"
      )
      const drained = yield* eventually(services, space, (status) => status.pending === 0)

      assert.isTrue(Option.isSome(reported), "the defect was reported as an unexpected failure")
      assert.isAbove(logs.messages().length, 0, "the defect was logged")
      assert.isTrue(Option.isSome(drained), "the retry drained the space")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "restarts once at once and then backs off while every call changes it with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* oneSpace(constructor)
      let generation = 0
      let rotating = true
      let calls = 0
      const counted = <A,>(answer: Effect.Effect<A, ReplicaError.ReplicaError>) =>
        Effect.suspend(() => {
          calls += 1
          if (rotating && calls <= 2000) generation += 1
          return answer
        })
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        credentialGeneration: Effect.sync(() => generation),
        submitBatch: (request) => counted(acceptSubmission(request)),
        pull: (request) => counted(emptyPage(services.crypto, request))
      }))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending")).pipe(VirtualTime.advanceUntil)
      yield* fiveMinutes
      const callsWhileRotating = calls

      rotating = false
      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.isAtMost(callsWhileRotating, 50, "server calls in five minutes of a generation that never settles")
      assert.isTrue(Option.isSome(drained), "the space drained once the generation settled")
    }, VirtualTime.scoped)
  )
})
