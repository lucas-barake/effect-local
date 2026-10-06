import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  captureErrors,
  type Constructor,
  constructors,
  describeExit,
  eventually,
  healthyRemote,
  idleRemote,
  installView,
  isOnlineDrained,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f41")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f42")
const thirdSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f43")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000f41")

interface Outcome {
  readonly kind: string
  readonly outcome: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly message: string
}

const outcomes: ReadonlyArray<Outcome> = [
  { kind: "a defect", outcome: Effect.die("subscriber died"), message: "Reactivity subscriber died" },
  { kind: "an interruption", outcome: Effect.interrupt, message: "Reactivity notification was interrupted" },
  {
    kind: "an interruption and a defect",
    outcome: Effect.ensuring(Effect.interrupt, Effect.die("subscriber died")),
    message: "Reactivity subscriber died"
  },
  {
    kind: "a failure",
    outcome: Effect.fail(new ReplicaError.ServerUnavailable()),
    message: "Reactivity notification failed"
  }
]

const withConstructor = (constructor: Constructor) => (outcome: Outcome) => Object.assign({ constructor }, outcome)

const rows = constructors.flatMap((constructor) => outcomes.map(withConstructor(constructor)))

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

describe("a notification that a Reactivity service ends with", () => {
  it.effect.each(rows)(
    "$kind leaves the space joined and its join succeeded with $constructor",
    Effect.fnUntraced(function*(row) {
      const errors = captureErrors()
      const services = yield* twoSpaces(row.constructor)
      const replica = yield* services.start(idleRemote)
      services.endInvalidationsWith(ReactivityKey.membership(thirdSpaceId), row.outcome)

      const joined = yield* replica.join(thirdSpaceId).pipe(Effect.provide(errors.layerLogs), within)
      const again = yield* within(replica.join(thirdSpaceId))
      const listed = yield* replica.spaces
      const aggregate = yield* replica.status

      assert.strictEqual(describeExit(joined), "succeeded", "the join")
      assert.strictEqual(describeExit(again), "succeeded", "a second join")
      assert.strictEqual(listed.length, 3)
      assert.strictEqual(aggregate.spaces, 3)
      assert.deepStrictEqual(errors.messages(), [row.message])
    }, VirtualTime.scoped)
  )

  it.effect.each(rows)(
    "$kind leaves the space left, its leave succeeded, and a second leave complete with $constructor",
    Effect.fnUntraced(function*(row) {
      const errors = captureErrors()
      const services = yield* twoSpaces(row.constructor)
      const replica = yield* services.start(idleRemote)
      services.endInvalidationsWith(ReactivityKey.membership(spaceId), row.outcome)

      const left = yield* replica.leave(spaceId).pipe(Effect.provide(errors.layerLogs), within)
      const again = yield* within(replica.leave(spaceId))
      const listed = yield* replica.spaces
      const aggregate = yield* replica.status
      const stored = yield* services.sql`SELECT space_id FROM effect_local_client_spaces WHERE space_id = ${spaceId}`

      assert.strictEqual(describeExit(left), "succeeded", "the leave")
      assert.strictEqual(describeExit(again), "succeeded", "a second leave")
      assert.strictEqual(stored.length, 0, "the membership row was deleted")
      assert.strictEqual(listed.length, 1)
      assert.strictEqual(aggregate.spaces, 1)
      assert.deepStrictEqual(errors.messages(), [row.message])
    }, VirtualTime.scoped)
  )

  it.effect.each(rows)(
    "$kind leaves the space active and its activation succeeded with $constructor",
    Effect.fnUntraced(function*(row) {
      const errors = captureErrors()
      const services = yield* twoSpaces(row.constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      services.endInvalidationsWith(ReactivityKey.activation(spaceId), row.outcome)

      const activated = yield* space.activate.pipe(Effect.provide(errors.layerLogs), within)
      const activation = yield* space.activation
      const again = yield* within(space.activate)
      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.strictEqual(describeExit(activated), "succeeded", "the activation")
      assert.strictEqual(activation, "Active")
      assert.strictEqual(describeExit(again), "succeeded", "a second activation")
      assert.isTrue(Option.isSome(drained), "the space came online")
      assert.deepStrictEqual(errors.messages(), [row.message, row.message])
    }, VirtualTime.scoped)
  )

  it.effect.each(rows)(
    "$kind leaves the space inactive, its deactivation succeeded, and a later activation complete with $constructor",
    Effect.fnUntraced(function*(row) {
      const errors = captureErrors()
      const services = yield* twoSpaces(row.constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      yield* space.activate
      assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
      services.endInvalidationsWith(ReactivityKey.activation(spaceId), row.outcome)

      const deactivated = yield* space.deactivate.pipe(Effect.provide(errors.layerLogs), within)
      const activation = yield* space.activation
      const aggregate = yield* replica.status
      services.endInvalidationsWith(ReactivityKey.activation(spaceId), Effect.void)
      const reactivated = yield* within(space.activate)

      assert.strictEqual(describeExit(deactivated), "succeeded", "the deactivation")
      assert.strictEqual(activation, "Inactive")
      assert.strictEqual(aggregate.counts.idle, 2)
      assert.strictEqual(describeExit(reactivated), "succeeded", "a later activation")
      assert.deepStrictEqual(errors.messages(), [row.message, row.message])
    }, VirtualTime.scoped)
  )

  it.effect.each(rows)(
    "$kind leaves the mutation committed, returned, and synced with $constructor",
    Effect.fnUntraced(function*(row) {
      const errors = captureErrors()
      const services = yield* twoSpaces(row.constructor)
      const replica = yield* services.start(healthyRemote(services)).pipe(Effect.provide(errors.layerLogs))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      yield* space.activate
      assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
      services.endInvalidationsWith(ReactivityKey.entity(spaceId, Domain.Todo.name, "first"), row.outcome)

      const mutated = yield* space.mutate(Domain.PutTodo, Domain.todo("first")).pipe(within)
      const next = yield* space.mutate(Domain.PutTodo, Domain.todo("second")).pipe(within)
      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.strictEqual(describeExit(mutated), "succeeded", "the mutation")
      assert.strictEqual(describeExit(next), "succeeded", "the next mutation")
      assert.isTrue(Option.isSome(drained), "both mutations synced")
      assert.isAbove(errors.messages().length, 0, "the outcome of the notification was logged")
      assert.deepStrictEqual(Array.from(new Set(errors.messages())), [row.message])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "an interruption when the runtime is ready still releases an activation that waited for it with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      const building = yield* services.holdStatement("SELECT desired_scope_json", true)
      const first = yield* space.activate.pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(building.entered)
      const waiting = yield* space.activate.pipe(Effect.forkChild({ startImmediately: true }))
      services.endInvalidationsWith(ReactivityKey.activation(spaceId), Effect.interrupt)
      yield* building.release

      const waited = yield* within(Fiber.join(waiting))
      const activated = yield* within(Fiber.join(first))

      assert.strictEqual(describeExit(waited), "succeeded", "the activation that waited")
      assert.strictEqual(describeExit(activated), "succeeded", "the first activation")
      assert.strictEqual(yield* space.activation, "Active")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "an interruption still releases a join that waited for the join it announced with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const inserting = yield* services.holdStatement("INSERT INTO effect_local_client_spaces", true)
      const first = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(inserting.entered)
      const waiting = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      services.endInvalidationsWith(ReactivityKey.membership(thirdSpaceId), Effect.interrupt)
      yield* inserting.release

      const waited = yield* within(Fiber.join(waiting))
      const joined = yield* within(Fiber.join(first))

      assert.strictEqual(describeExit(waited), "succeeded", "the join that waited")
      assert.strictEqual(describeExit(joined), "succeeded", "the first join")
    }, VirtualTime.scoped)
  )
})
