import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  type Constructor,
  constructors,
  describeExit,
  eventually,
  healthyRemote,
  installView,
  isOnlineDrained,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f51")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f52")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000f51")

interface Outcome {
  readonly kind: string
  readonly outcome: Effect.Effect<void, ReplicaError.ReplicaError>
}

const outcomes: ReadonlyArray<Outcome> = [
  { kind: "a defect", outcome: Effect.die("subscriber died") },
  { kind: "an interruption", outcome: Effect.interrupt },
  { kind: "a failure", outcome: Effect.fail(new ReplicaError.ServerUnavailable()) }
]

const withConstructor = (constructor: Constructor) => (outcome: Outcome) => Object.assign({ constructor }, outcome)

const endedRows = constructors.flatMap((constructor) => outcomes.map(withConstructor(constructor)))

const twoSpaces = (constructor: Constructor) =>
  BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId, otherSpaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })

const todosOnly = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const nowhere = "no key"

const consistent = Effect.fnUntraced(function*(replica: Replica.Replica["Service"], space: Replica.Space) {
  const activation = yield* space.activation
  const status = yield* space.status
  const aggregate = yield* replica.status
  return `${activation}, status ${status._tag}, aggregate online ${aggregate.counts.online} idle ${aggregate.counts.idle}`
})

describe("an operation whose notification a Reactivity service ended early", () => {
  it.effect.each(endedRows.flatMap((row) => [
    Object.assign({ key: ReactivityKey.activation(spaceId), name: "the activation" }, row),
    Object.assign({ key: ReactivityKey.scope(spaceId), name: "the scope" }, row)
  ]))(
    "leaves a scope change with $kind on $name applied, the space active, and usable with $constructor",
    Effect.fnUntraced(function*(row) {
      const services = yield* twoSpaces(row.constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      yield* space.activate
      assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
      services.endInvalidationsWith(row.key, row.outcome)

      const changed = yield* within(space.setScope(todosOnly))
      services.endInvalidationsWith(nowhere, Effect.void)
      const scope = yield* space.scope
      const activation = yield* space.activation
      const mutated = yield* space.mutate(Domain.PutTodo, Domain.todo("after")).pipe(within)
      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.strictEqual(describeExit(changed), "succeeded", "the scope change")
      assert.deepStrictEqual(scope.models, todosOnly.models)
      assert.strictEqual(describeExit(mutated), "succeeded", "a mutation afterwards")
      assert.isTrue(Option.isSome(drained), "the mutation synced")
      assert.strictEqual(activation, "Active", "the space was active again when the scope change ended")
    }, VirtualTime.scoped)
  )

  it.effect.each(endedRows.flatMap((row) => [
    Object.assign({ key: ReactivityKey.status(spaceId), name: "the status" }, row),
    Object.assign({ key: ReactivityKey.aggregateStatus, name: "the aggregate status" }, row)
  ]))(
    "leaves an activation with $kind on $name consistent and its foreground place reusable with $constructor",
    Effect.fnUntraced(function*(row) {
      const services = yield* BackgroundReplica.services({
        constructor: row.constructor,
        clientId,
        initialSpaces: [spaceId, otherSpaceId],
        maximumActiveSpaces: 4,
        foregroundActiveSpaces: 1,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      const other = yield* replica.space(otherSpaceId)
      services.endInvalidationsWith(row.key, row.outcome)

      const activated = yield* within(space.activate)
      services.endInvalidationsWith(nowhere, Effect.void)
      const drained = yield* eventually(services, space, isOnlineDrained)
      const state = yield* consistent(replica, space)
      const taken = yield* within(other.activate)
      const mutated = yield* space.mutate(Domain.PutTodo, Domain.todo("after")).pipe(within)
      const again = yield* eventually(services, space, isOnlineDrained)

      assert.strictEqual(describeExit(activated), "succeeded", "the activation")
      assert.strictEqual(describeExit(taken), "succeeded", "the other space took the foreground place")
      assert.strictEqual(describeExit(mutated), "succeeded", "the first space took it back for a mutation")
      assert.isTrue(Option.isSome(again), "the mutation synced")
      assert.strictEqual(state.split(",")[0], "Active", state)
      assert.isTrue(Option.isSome(drained), "the space came online after its activation")
    }, VirtualTime.scoped)
  )

  it.effect.each(endedRows)(
    "commits and syncs a mutation whose implicit activation was ended with $kind with $constructor",
    Effect.fnUntraced(function*(row) {
      const services = yield* twoSpaces(row.constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      services.endInvalidationsWith(ReactivityKey.activation(spaceId), row.outcome)

      const mutated = yield* space.mutate(Domain.PutTodo, Domain.todo("first")).pipe(within)
      services.endInvalidationsWith(nowhere, Effect.void)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))
      const status = yield* space.status
      const retried = yield* space.mutate(Domain.PutTodo, Domain.todo("second")).pipe(within)

      assert.strictEqual(describeExit(mutated), "succeeded", "the mutation")
      assert.strictEqual(`${status._tag}, pending ${status.pending}`, "Online, pending 0")
      assert.strictEqual(describeExit(retried), "succeeded", "a later mutation")
    }, VirtualTime.scoped)
  )

  it.effect.each(endedRows)(
    "releases a space that its caller acquired in a scope when the activation was ended with $kind with $constructor",
    Effect.fnUntraced(function*(row) {
      const services = yield* twoSpaces(row.constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      services.endInvalidationsWith(ReactivityKey.activation(spaceId), row.outcome)
      const scope = yield* Scope.make()

      const acquired = yield* Effect.acquireRelease(space.activate, () => Effect.exit(space.deactivate)).pipe(
        Scope.provide(scope),
        within
      )
      services.endInvalidationsWith(nowhere, Effect.void)
      yield* Scope.close(scope, Exit.void)
      const activation = yield* space.activation

      assert.strictEqual(describeExit(acquired), "succeeded", "the acquisition")
      assert.strictEqual(activation, "Inactive", "nothing the caller did not acquire stayed active")
    }, VirtualTime.scoped)
  )

  it.effect.each(endedRows)(
    "drains a background space once the Reactivity service stops ending its activation with $kind with $constructor",
    Effect.fnUntraced(function*(row) {
      const services = yield* twoSpaces(row.constructor)
      yield* BackgroundReplica.seedPending(services, [spaceId])
      services.endInvalidationsWith(ReactivityKey.activation(spaceId), row.outcome)
      const replica = yield* services.start(healthyRemote(services))
      const space = yield* replica.space(spaceId)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))
      const whileEnding = yield* consistent(replica, space)
      services.endInvalidationsWith(nowhere, Effect.void)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("5 minutes"))
      const status = yield* space.status
      const after = yield* consistent(replica, space)

      assert.strictEqual(`${status._tag}, pending ${status.pending}`, "Idle, pending 0", whileEnding)
      assert.strictEqual(after, "Inactive, status Idle, aggregate online 0 idle 2")
    }, VirtualTime.scoped)
  )
})
