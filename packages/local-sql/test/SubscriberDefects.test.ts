import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Logger from "effect/Logger"
import * as Option from "effect/Option"
import * as References from "effect/References"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  awaitSpaceStatusWhere,
  type Constructor,
  constructors,
  emptyPage,
  idleRemote,
  makeAttempts,
  viewId
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000e01")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000e02")
const thirdSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000e03")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000e01")

const spaces = (
  constructor: Constructor,
  initialSpaces: ReadonlyArray<Identity.SpaceId>,
  foregroundActiveSpaces = 2
) =>
  BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces,
    maximumActiveSpaces: 4,
    foregroundActiveSpaces,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })

const firstTodo = Domain.todo("first")
const todosOnly = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })

const twoSpaces = (constructor: Constructor) => spaces(constructor, [spaceId, otherSpaceId])

const captureLogs = () => {
  const defects: Array<string> = []
  const logger = Logger.make<unknown, void>((entry) => {
    let message: unknown = entry.message
    if (Array.isArray(message)) message = message[0]
    if (entry.logLevel !== "Error" || message !== "Reactivity subscriber died") return
    if (!Cause.hasDies(entry.cause)) defects.push("logged without the defect")
    else defects.push(String(entry.fiber.getRef(References.CurrentLogAnnotations)["reactivity.key"]))
  })
  return { layerLogs: Logger.layer([logger]), subscriberDefects: () => defects }
}

const subscribe = (services: BackgroundReplica.Services, key: string, sibling?: string) => {
  let untilThrow = 0
  let throws = 0
  let earlier = 0
  let owed = false
  let siblingReached = 0
  services.reactivity.registerUnsafe([key], () => {
    earlier += 1
  })
  services.reactivity.registerUnsafe([key], () => {
    if (untilThrow === 0) return
    untilThrow -= 1
    if (untilThrow > 0) return
    throws += 1
    owed = true
    decodeURIComponent("%")
  })
  if (sibling !== undefined) {
    services.reactivity.registerUnsafe([sibling], () => {
      if (!owed) return
      owed = false
      siblingReached += 1
    })
  }
  return {
    throwAt: (nth: number) => {
      earlier = 0
      untilThrow = nth
    },
    throws: () => throws,
    earlier: () => earlier,
    siblingReached: () => siblingReached
  }
}

const eventually = (
  services: BackgroundReplica.Services,
  space: Replica.Space,
  matches: (status: ReplicaStatus.SpaceStatus) => boolean
) =>
  awaitSpaceStatusWhere(space, services.reactivity, matches).pipe(
    Effect.scoped,
    VirtualTime.advanceUntil,
    Effect.timeoutOption("5 minutes")
  )

const settle = (duration: Duration.Input) => VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption(duration))

const within = <A, E extends { readonly _tag: string },>(effect: Effect.Effect<A, E>) =>
  effect.pipe(Effect.exit, Effect.timeoutOption("5 minutes"), VirtualTime.advanceUntil)

const isOnlineDrained = (status: ReplicaStatus.SpaceStatus) => status._tag === "Online" && status.pending === 0

const isOfflinePending = (status: ReplicaStatus.SpaceStatus) => status._tag === "Offline" && status.pending === 1

const describeStatus = (status: ReplicaStatus.ReplicaStatus) => {
  if (status._tag === "Failed") return `Failed: ${status.message}, pending ${status.pending}`
  return `${status._tag}, pending ${status.pending}`
}

const describeSettled = <A, E extends { readonly _tag: string },>(exit: Exit.Exit<A, E>) => {
  if (Exit.isSuccess(exit)) return "succeeded"
  if (Cause.hasDies(exit.cause)) return "died"
  return "failed"
}

const describeExit = <A, E extends { readonly _tag: string },>(exit: Option.Option<Exit.Exit<A, E>>) => {
  if (Option.isNone(exit)) return "never completed"
  return describeSettled(exit.value)
}

const healthyRemote = (services: BackgroundReplica.Services, isOffline: () => boolean) =>
  SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: (request) => {
      if (isOffline()) return Effect.fail(new ReplicaError.ServerUnavailable())
      return acceptSubmission(request)
    },
    pull: (request) => emptyPage(services.crypto, request)
  })

const hangingThenDrain = (
  services: BackgroundReplica.Services,
  attempts: { readonly record: Effect.Effect<void>; readonly count: () => number }
) =>
  SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: acceptSubmission,
    pull: (request) => {
      if (request.spaceId !== spaceId) return emptyPage(services.crypto, request)
      if (attempts.count() === 0) return Effect.andThen(attempts.record, Effect.never)
      return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
    }
  })

const installView = (services: BackgroundReplica.Services) =>
  services.sql`UPDATE effect_local_client_spaces SET replication_view_id = ${viewId}, replication_view_revision = 0`

const onlineSpace = Effect.fnUntraced(function*(services: BackgroundReplica.Services, isOffline: () => boolean) {
  const replica = yield* services.start(healthyRemote(services, isOffline))
  yield* installView(services)
  const space = yield* replica.space(spaceId)
  yield* space.activate
  assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
  return { replica, space }
})

const backgroundRows = constructors.flatMap((constructor) => [
  {
    constructor,
    name: "the aggregate status",
    key: ReactivityKey.aggregateStatus,
    nth: 1,
    sibling: undefined
  },
  {
    constructor,
    name: "the space status",
    key: ReactivityKey.status(spaceId),
    nth: 1,
    sibling: undefined
  },
  {
    constructor,
    name: "the activation when the turn started",
    key: ReactivityKey.activation(spaceId),
    nth: 1,
    sibling: ReactivityKey.status(spaceId)
  },
  {
    constructor,
    name: "the activation when the turn released the space",
    key: ReactivityKey.activation(spaceId),
    nth: 3,
    sibling: ReactivityKey.status(spaceId)
  }
])

describe("a subscriber that throws during a background turn", () => {
  it.effect.each(backgroundRows)(
    "drains the space and leaves it inactive when the subscriber of $name threw with $constructor",
    Effect.fnUntraced(function*(row) {
      const logs = captureLogs()
      const result = yield* Effect.gen(function*() {
        const services = yield* twoSpaces(row.constructor)
        yield* BackgroundReplica.seedPending(services, [spaceId])
        const subscriber = subscribe(services, row.key, row.sibling)
        subscriber.throwAt(row.nth)
        const replica = yield* services.start(healthyRemote(services, () => false))
        const space = yield* replica.space(spaceId)
        yield* settle("5 minutes")
        const status = yield* space.status
        const activation = yield* space.activation
        const activated = yield* within(space.activate)
        return { subscriber, status, activation, activated }
      }).pipe(Effect.provide(logs.layerLogs))

      assert.strictEqual(result.subscriber.throws(), 1, "the subscriber threw once")
      assert.strictEqual(describeStatus(result.status), "Idle, pending 0")
      assert.strictEqual(result.activation, "Inactive")
      assert.strictEqual(describeExit(result.activated), "succeeded", "a later activation")
      assert.deepStrictEqual(logs.subscriberDefects(), [row.key])
      assert.isAbove(result.subscriber.earlier(), 0, "an earlier subscriber of the same key was notified")
      if (row.sibling !== undefined) {
        assert.strictEqual(result.subscriber.siblingReached(), 1, "the next key of the same notification")
      }
    }, VirtualTime.scoped)
  )
})

const activationRows = constructors.flatMap((constructor) => [
  {
    constructor,
    name: "the activation when the activation started",
    key: ReactivityKey.activation(spaceId),
    nth: 1,
    sibling: ReactivityKey.status(spaceId)
  },
  {
    constructor,
    name: "the activation when the runtime was ready",
    key: ReactivityKey.activation(spaceId),
    nth: 2,
    sibling: ReactivityKey.status(spaceId)
  },
  {
    constructor,
    name: "the space status when the activation started",
    key: ReactivityKey.status(spaceId),
    nth: 1,
    sibling: undefined
  },
  {
    constructor,
    name: "the space status when the replication view was recorded",
    key: ReactivityKey.status(spaceId),
    nth: 2,
    sibling: undefined
  },
  {
    constructor,
    name: "the aggregate status",
    key: ReactivityKey.aggregateStatus,
    nth: 1,
    sibling: undefined
  }
])

describe("a subscriber that throws during an activation", () => {
  it.effect.each(activationRows)(
    "activates the space and syncs its next mutation when the subscriber of $name threw with $constructor",
    Effect.fnUntraced(function*(row) {
      const logs = captureLogs()
      const result = yield* Effect.gen(function*() {
        const services = yield* twoSpaces(row.constructor)
        const replica = yield* services.start(healthyRemote(services, () => false))
        yield* installView(services)
        const space = yield* replica.space(spaceId)
        const subscriber = subscribe(services, row.key, row.sibling)
        subscriber.throwAt(row.nth)
        const activated = yield* within(space.activate)
        const threwDuringActivation = subscriber.throws()
        const activation = yield* space.activation
        const mutated = yield* within(space.mutate(Domain.PutTodo, firstTodo))
        yield* settle("5 minutes")
        const status = yield* space.status
        return { subscriber, activated, threwDuringActivation, activation, mutated, status }
      }).pipe(Effect.provide(logs.layerLogs))

      assert.strictEqual(result.threwDuringActivation, 1, "the subscriber threw during the activation")
      assert.strictEqual(describeExit(result.activated), "succeeded", "the activation")
      assert.strictEqual(result.activation, "Active")
      assert.strictEqual(describeExit(result.mutated), "succeeded", "the mutation")
      assert.strictEqual(describeStatus(result.status), "Online, pending 0")
      assert.deepStrictEqual(logs.subscriberDefects(), [row.key])
      assert.isAbove(result.subscriber.earlier(), 0, "an earlier subscriber of the same key was notified")
      if (row.sibling !== undefined) {
        assert.strictEqual(result.subscriber.siblingReached(), 1, "the next key of the same notification")
      }
    }, VirtualTime.scoped)
  )
})

const deactivationRows = constructors.flatMap((constructor) => [
  {
    constructor,
    name: "the activation when the deactivation started",
    key: ReactivityKey.activation(spaceId),
    nth: 1,
    sibling: ReactivityKey.status(spaceId)
  },
  {
    constructor,
    name: "the activation when the runtime had closed",
    key: ReactivityKey.activation(spaceId),
    nth: 2,
    sibling: ReactivityKey.status(spaceId)
  },
  {
    constructor,
    name: "the aggregate status after the runtime had closed",
    key: ReactivityKey.aggregateStatus,
    nth: 1,
    sibling: undefined
  }
])

describe("a subscriber that throws during a deactivation", () => {
  it.effect.each(deactivationRows)(
    "deactivates the space and syncs its pending mutation when the subscriber of $name threw with $constructor",
    Effect.fnUntraced(function*(row) {
      const logs = captureLogs()
      const result = yield* Effect.gen(function*() {
        const services = yield* twoSpaces(row.constructor)
        let offline = false
        const { space } = yield* onlineSpace(services, () => offline)
        offline = true
        yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
        assert.isTrue(Option.isSome(yield* eventually(services, space, isOfflinePending)), "the mutation was pending")
        offline = false
        const subscriber = subscribe(services, row.key, row.sibling)
        subscriber.throwAt(row.nth)
        const deactivated = yield* within(space.deactivate)
        const activation = yield* space.activation
        yield* settle("5 minutes")
        const status = yield* space.status
        const settled = yield* space.activation
        return { subscriber, deactivated, activation, status, settled }
      }).pipe(Effect.provide(logs.layerLogs))

      assert.strictEqual(result.subscriber.throws(), 1, "the subscriber threw once")
      assert.strictEqual(describeExit(result.deactivated), "succeeded", "the deactivation")
      assert.strictEqual(result.activation, "Inactive")
      assert.strictEqual(describeStatus(result.status), "Idle, pending 0")
      assert.strictEqual(result.settled, "Inactive")
      assert.deepStrictEqual(logs.subscriberDefects(), [row.key])
      assert.isAbove(result.subscriber.earlier(), 0, "an earlier subscriber of the same key was notified")
      if (row.sibling !== undefined) {
        assert.strictEqual(result.subscriber.siblingReached(), 1, "the next key of the same notification")
      }
    }, VirtualTime.scoped)
  )
})

const scopeRows = constructors.flatMap((constructor) => [
  {
    constructor,
    name: "the activation",
    key: ReactivityKey.activation(spaceId),
    sibling: ReactivityKey.status(spaceId)
  },
  {
    constructor,
    name: "the scope",
    key: ReactivityKey.scope(spaceId),
    sibling: undefined
  }
])

describe("a subscriber that throws during a scope change", () => {
  it.effect.each(scopeRows)(
    "changes the scope, reactivates the space and syncs its next mutation when the subscriber of $name threw with $constructor",
    Effect.fnUntraced(function*(row) {
      const logs = captureLogs()
      const result = yield* Effect.gen(function*() {
        const services = yield* twoSpaces(row.constructor)
        const { space } = yield* onlineSpace(services, () => false)
        const subscriber = subscribe(services, row.key, row.sibling)
        subscriber.throwAt(1)
        const changed = yield* within(space.setScope(todosOnly))
        const threwDuringChange = subscriber.throws()
        const activation = yield* space.activation
        const mutated = yield* within(space.mutate(Domain.PutTodo, firstTodo))
        yield* settle("5 minutes")
        const status = yield* space.status
        return { subscriber, changed, threwDuringChange, activation, mutated, status }
      }).pipe(Effect.provide(logs.layerLogs))

      assert.strictEqual(result.threwDuringChange, 1, "the subscriber threw during the scope change")
      assert.strictEqual(describeExit(result.changed), "succeeded", "the scope change")
      assert.strictEqual(result.activation, "Active")
      assert.strictEqual(describeExit(result.mutated), "succeeded", "the mutation")
      assert.strictEqual(describeStatus(result.status), "Online, pending 0")
      assert.deepStrictEqual(logs.subscriberDefects(), [row.key])
      assert.isAbove(result.subscriber.earlier(), 0, "an earlier subscriber of the same key was notified")
      if (row.sibling !== undefined) {
        assert.strictEqual(result.subscriber.siblingReached(), 1, "the next key of the same notification")
      }
    }, VirtualTime.scoped)
  )
})

const mutationRows = constructors.flatMap((constructor) => [
  {
    constructor,
    name: "the space status",
    key: ReactivityKey.status(spaceId),
    sibling: undefined
  },
  {
    constructor,
    name: "the pending mutations",
    key: ReactivityKey.pending(spaceId),
    sibling: ReactivityKey.status(spaceId)
  },
  {
    constructor,
    name: "the aggregate status",
    key: ReactivityKey.aggregateStatus,
    sibling: undefined
  }
])

describe("a subscriber that throws while a commit is announced", () => {
  it.effect.each(mutationRows)(
    "returns the committed mutation and syncs it when the subscriber of $name threw with $constructor",
    Effect.fnUntraced(function*(row) {
      const logs = captureLogs()
      const result = yield* Effect.gen(function*() {
        const services = yield* twoSpaces(row.constructor)
        const { space } = yield* onlineSpace(services, () => false)
        const subscriber = subscribe(services, row.key, row.sibling)
        subscriber.throwAt(1)
        const mutated = yield* within(space.mutate(Domain.PutTodo, firstTodo))
        const threwDuringCommit = subscriber.throws()
        yield* settle("4 minutes")
        const status = yield* space.status
        return { subscriber, mutated, threwDuringCommit, status }
      }).pipe(Effect.provide(logs.layerLogs))

      assert.strictEqual(result.threwDuringCommit, 1, "the subscriber threw while the commit was announced")
      assert.strictEqual(describeExit(result.mutated), "succeeded", "the mutation")
      assert.strictEqual(describeStatus(result.status), "Online, pending 0")
      assert.deepStrictEqual(logs.subscriberDefects(), [row.key])
      assert.isAbove(result.subscriber.earlier(), 0, "an earlier subscriber of the same key was notified")
      if (row.sibling !== undefined) {
        assert.strictEqual(result.subscriber.siblingReached(), 1, "the next key of the same notification")
      }
    }, VirtualTime.scoped)
  )
})

const joinRows = constructors.flatMap((constructor) => [
  {
    constructor,
    name: "the membership",
    key: ReactivityKey.membership(thirdSpaceId),
    sibling: ReactivityKey.spaces
  },
  {
    constructor,
    name: "the space list",
    key: ReactivityKey.spaces,
    sibling: undefined
  },
  {
    constructor,
    name: "the aggregate status",
    key: ReactivityKey.aggregateStatus,
    sibling: ReactivityKey.membership(thirdSpaceId)
  }
])

describe("a subscriber that throws while a join is announced", () => {
  it.effect.each(joinRows)(
    "joins the space once and syncs its first mutation when the subscriber of $name threw with $constructor",
    Effect.fnUntraced(function*(row) {
      const logs = captureLogs()
      const result = yield* Effect.gen(function*() {
        const services = yield* twoSpaces(row.constructor)
        const replica = yield* services.start(healthyRemote(services, () => false))
        const subscriber = subscribe(services, row.key, row.sibling)
        subscriber.throwAt(1)
        const joined = yield* within(replica.join(thirdSpaceId))
        const aggregate = yield* replica.status
        const again = yield* replica.join(thirdSpaceId)
        const listed = yield* replica.spaces
        yield* installView(services)
        yield* again.activate
        const mutated = yield* within(again.mutate(Domain.PutTodo, firstTodo))
        yield* settle("5 minutes")
        const status = yield* again.status
        return { subscriber, joined, aggregate, again, listed, mutated, status }
      }).pipe(Effect.provide(logs.layerLogs))

      assert.strictEqual(result.subscriber.throws(), 1, "the subscriber threw once")
      assert.strictEqual(describeExit(result.joined), "succeeded", "the join")
      assert.strictEqual(result.aggregate.spaces, 3)
      assert.strictEqual(result.listed.length, 3)
      assert.isTrue(result.listed.includes(result.again), "the second join returned the listed handle")
      assert.strictEqual(describeExit(result.mutated), "succeeded", "the mutation")
      assert.strictEqual(describeStatus(result.status), "Online, pending 0")
      assert.deepStrictEqual(logs.subscriberDefects(), [row.key])
      assert.isAbove(result.subscriber.earlier(), 0, "an earlier subscriber of the same key was notified")
      if (row.sibling !== undefined) {
        assert.strictEqual(result.subscriber.siblingReached(), 1, "the next key of the same notification")
      }
    }, VirtualTime.scoped)
  )
})

const leaveRows = constructors.flatMap((constructor) => [
  {
    constructor,
    name: "the membership",
    key: ReactivityKey.membership(spaceId),
    sibling: ReactivityKey.spaces
  },
  {
    constructor,
    name: "the space list",
    key: ReactivityKey.spaces,
    sibling: undefined
  },
  {
    constructor,
    name: "the aggregate status",
    key: ReactivityKey.aggregateStatus,
    sibling: ReactivityKey.membership(spaceId)
  }
])

describe("a subscriber that throws while a leave is announced", () => {
  it.effect.each(leaveRows)(
    "leaves the space and lets a join that waited for the leave complete when the subscriber of $name threw with $constructor",
    Effect.fnUntraced(function*(row) {
      const logs = captureLogs()
      const result = yield* Effect.gen(function*() {
        const services = yield* twoSpaces(row.constructor)
        const replica = yield* services.start(idleRemote)
        const subscriber = subscribe(services, row.key, row.sibling)
        const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces", true)
        const leaving = yield* replica.leave(spaceId).pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
        yield* removal.entered
        const joining = yield* replica.join(spaceId).pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
        const leavingAgain = yield* replica.leave(spaceId).pipe(
          Effect.exit,
          Effect.forkChild({ startImmediately: true })
        )
        subscriber.throwAt(1)
        yield* removal.release
        const left = yield* Fiber.join(leaving)
        const leftAgain = yield* Fiber.join(leavingAgain)
        const joined = yield* Fiber.join(joining)
        const aggregate = yield* replica.status
        const listed = yield* replica.spaces
        return { subscriber, left, leftAgain, joined, aggregate, listed }
      }).pipe(Effect.provide(logs.layerLogs))

      assert.strictEqual(result.subscriber.throws(), 1, "the subscriber threw once")
      assert.strictEqual(describeSettled(result.left), "succeeded", "the leave")
      assert.strictEqual(describeSettled(result.leftAgain), "succeeded", "the second leave")
      assert.strictEqual(describeSettled(result.joined), "succeeded", "the join that waited for the leave")
      assert.strictEqual(result.aggregate.spaces, result.listed.length)
      assert.deepStrictEqual(logs.subscriberDefects(), [row.key])
      assert.isAbove(result.subscriber.earlier(), 0, "an earlier subscriber of the same key was notified")
      if (row.sibling !== undefined) {
        assert.strictEqual(result.subscriber.siblingReached(), 1, "the next key of the same notification")
      }
    }, VirtualTime.scoped)
  )
})

describe("background workers after a subscriber threw during the turns of two spaces", () => {
  it.effect.each(constructors)(
    "drain a third space in the background with %s",
    Effect.fnUntraced(function*(constructor) {
      const logs = captureLogs()
      const result = yield* Effect.gen(function*() {
        const services = yield* spaces(constructor, [spaceId, otherSpaceId, thirdSpaceId])
        yield* BackgroundReplica.seedPending(services, [spaceId, otherSpaceId])
        const first = subscribe(services, ReactivityKey.status(spaceId))
        const second = subscribe(services, ReactivityKey.status(otherSpaceId))
        first.throwAt(1)
        second.throwAt(1)
        let offline = false
        const replica = yield* services.start(healthyRemote(services, () => offline))
        const wedged = yield* replica.space(spaceId)
        const third = yield* replica.space(thirdSpaceId)
        yield* settle("10 minutes")
        yield* services.sql`UPDATE effect_local_client_spaces
          SET replication_view_id = ${viewId}, replication_view_revision = 0 WHERE space_id = ${thirdSpaceId}`
        yield* third.activate
        offline = true
        yield* third.mutate(Domain.PutTodo, Domain.todo("third"))
        assert.isTrue(Option.isSome(yield* eventually(services, third, isOfflinePending)), "the mutation was pending")
        yield* third.deactivate
        offline = false
        yield* settle("5 minutes")
        return {
          throws: first.throws() + second.throws(),
          wedged: yield* wedged.status,
          third: yield* third.status
        }
      }).pipe(Effect.provide(logs.layerLogs))

      assert.strictEqual(result.throws, 2, "each subscriber threw once")
      assert.strictEqual(describeStatus(result.wedged), "Idle, pending 0")
      assert.strictEqual(describeStatus(result.third), "Idle, pending 0")
      assert.deepStrictEqual(logs.subscriberDefects(), [
        ReactivityKey.status(spaceId),
        ReactivityKey.status(otherSpaceId)
      ])
    }, VirtualTime.scoped)
  )
})

const everyRows = constructors.flatMap((constructor) => [
  { constructor, name: "the aggregate status", key: ReactivityKey.aggregateStatus },
  { constructor, name: "the space status", key: ReactivityKey.status(spaceId) },
  { constructor, name: "the activation", key: ReactivityKey.activation(spaceId) },
  { constructor, name: "the pending mutations", key: ReactivityKey.pending(spaceId) },
  { constructor, name: "the scope", key: ReactivityKey.scope(spaceId) },
  { constructor, name: "the membership", key: ReactivityKey.membership(spaceId) },
  { constructor, name: "the space list", key: ReactivityKey.spaces }
])

describe("a subscriber that throws on every notification", () => {
  it.effect.each(everyRows)(
    "does not stop a space from syncing or changing state when it subscribes to $name with $constructor",
    Effect.fnUntraced(function*(row) {
      const logs = captureLogs()
      const result = yield* Effect.gen(function*() {
        const services = yield* twoSpaces(row.constructor)
        yield* BackgroundReplica.seedPending(services, [spaceId])
        let throws = 0
        services.reactivity.registerUnsafe([row.key], () => {
          throws += 1
          decodeURIComponent("%")
        })
        let offline = true
        const replica = yield* services.start(healthyRemote(services, () => offline))
        const space = yield* replica.space(spaceId)
        yield* settle("10 seconds")
        const whileOffline = yield* space.status
        offline = false
        yield* settle("5 minutes")
        const background = yield* space.status
        const activated = yield* within(space.activate)
        const mutated = yield* within(space.mutate(Domain.PutTodo, firstTodo))
        yield* settle("5 minutes")
        const foreground = yield* space.status
        const changed = yield* within(space.setScope(todosOnly))
        const deactivated = yield* within(space.deactivate)
        const left = yield* within(replica.leave(spaceId))
        const joined = yield* within(replica.join(spaceId))
        return {
          throws,
          whileOffline,
          background,
          activated,
          mutated,
          foreground,
          changed,
          deactivated,
          left,
          joined,
          aggregate: yield* replica.status
        }
      }).pipe(Effect.provide(logs.layerLogs))

      assert.isAbove(result.throws, 0, "the subscriber threw")
      assert.strictEqual(result.whileOffline.pending, 1)
      assert.strictEqual(describeStatus(result.background), "Idle, pending 0")
      assert.strictEqual(describeExit(result.activated), "succeeded", "the activation")
      assert.strictEqual(describeExit(result.mutated), "succeeded", "the mutation")
      assert.strictEqual(describeStatus(result.foreground), "Online, pending 0")
      assert.strictEqual(describeExit(result.changed), "succeeded", "the scope change")
      assert.strictEqual(describeExit(result.deactivated), "succeeded", "the deactivation")
      assert.strictEqual(describeExit(result.left), "succeeded", "the leave")
      assert.strictEqual(describeExit(result.joined), "succeeded", "the join")
      assert.strictEqual(result.aggregate.spaces, 2)
      assert.strictEqual(logs.subscriberDefects().length, result.throws, "every throw was logged")
    }, VirtualTime.scoped)
  )
})

describe("a notification that is still being delivered", () => {
  it.effect.each(constructors)(
    "does not hold back the background sync of a space that was deactivated with a pending mutation with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      let offline = false
      const { space } = yield* onlineSpace(services, () => offline)
      offline = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      assert.isTrue(Option.isSome(yield* eventually(services, space, isOfflinePending)), "the mutation was pending")
      offline = false
      const delivery = yield* services.holdInvalidation(ReactivityKey.aggregateStatus)
      delivery.arm(1)
      const deactivating = yield* space.deactivate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(delivery.entered)

      yield* settle("5 minutes")
      const status = yield* space.status
      const delivering = deactivating.pollUnsafe() === undefined
      yield* delivery.release
      const deactivated = yield* Fiber.join(deactivating)

      assert.isTrue(delivering, "the notification of the deactivation was still being delivered")
      assert.strictEqual(describeStatus(status), "Idle, pending 0")
      assert.isTrue(Exit.isSuccess(deactivated), "the deactivation")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "of the space status does not hold back the aggregate status of a space that came online with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const delivery = yield* services.holdInvalidation(ReactivityKey.status(spaceId))
      let pulled = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (request.spaceId === spaceId && !pulled) {
            pulled = true
            delivery.arm(1)
          }
          return emptyPage(services.crypto, request)
        }
      }))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      yield* space.activate
      yield* VirtualTime.advanceUntil(delivery.entered)

      const aggregate = yield* replica.status
      yield* delivery.release

      assert.strictEqual(aggregate.counts.online, 1)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "of the space status does not hold back the aggregate status of a space whose sync failed with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const delivery = yield* services.holdInvalidation(ReactivityKey.status(spaceId))
      let offline = false
      const { replica, space } = yield* onlineSpace(services, () => {
        if (offline) delivery.arm(2)
        return offline
      })
      offline = true
      const mutating = yield* space.mutate(Domain.PutTodo, firstTodo).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* VirtualTime.advanceUntil(delivery.entered)

      const aggregate = yield* replica.status
      yield* delivery.release
      const mutated = yield* within(Fiber.join(mutating))

      assert.strictEqual(aggregate.counts.offline, 1)
      assert.strictEqual(describeExit(mutated), "succeeded", "the mutation")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not hold back the background retry of a space whose foreground takeover failed to build with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      yield* BackgroundReplica.seedPending(services, [spaceId])
      const attempts = yield* makeAttempts
      const replica = yield* services.start(hangingThenDrain(services, attempts))
      const space = yield* replica.space(spaceId)
      yield* VirtualTime.advanceUntil(attempts.reached(1))
      const delivery = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      delivery.arm(2)
      services.lockNext("SELECT desired_scope_json")
      const activating = yield* space.activate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(delivery.entered)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      const delivering = activating.pollUnsafe() === undefined
      yield* delivery.release
      const activated = yield* Fiber.join(activating)

      assert.isTrue(delivering, "the notification of the failed takeover was still being delivered")
      assert.isTrue(Option.isSome(retried), "the background turn of the retired runtime ran again")
      assert.strictEqual(describeSettled(activated), "failed", "the foreground activation")
    }, VirtualTime.scoped)
  )

  it.effect(
    "announces the pending count of a commit that outlived its caller while the space was still closing",
    Effect.fnUntraced(function*() {
      const warnings: Array<string> = []
      const logger = Logger.make<unknown, void>((entry) => {
        let message: unknown = entry.message
        if (Array.isArray(message)) message = message[0]
        if (entry.logLevel === "Warn") warnings.push(String(message))
      })
      const result = yield* Effect.gen(function*() {
        const services = yield* twoSpaces("layer")
        const { replica, space } = yield* onlineSpace(services, () => false)
        const committing = yield* services.holdStatement("INSERT INTO effect_local_client_pending_data", true)
        const mutating = yield* space.mutate(Domain.PutTodo, firstTodo).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* VirtualTime.advanceUntil(committing.entered)
        yield* Fiber.interrupt(mutating)
        const deactivating = yield* space.deactivate.pipe(Effect.forkChild({ startImmediately: true }))
        yield* settle("1 second")
        const delivery = yield* services.holdInvalidation(ReactivityKey.aggregateStatus)
        delivery.arm(1)
        yield* committing.release
        yield* VirtualTime.advanceUntil(delivery.entered)
        const activation = yield* space.activation
        const aggregate = yield* replica.status
        yield* delivery.release
        const deactivated = yield* within(Fiber.join(deactivating))
        yield* settle("5 minutes")
        return { activation, aggregate, deactivated, status: yield* space.status }
      }).pipe(Effect.provide(Logger.layer([logger])))

      assert.deepStrictEqual(warnings, ["Committed local mutations could not schedule reconciliation"])
      assert.strictEqual(result.activation, "Deactivating")
      assert.strictEqual(result.aggregate.totalPending, 1)
      assert.strictEqual(describeExit(result.deactivated), "succeeded", "the deactivation")
      assert.strictEqual(describeStatus(result.status), "Idle, pending 0")
    }, VirtualTime.scoped)
  )
})

const takeoverRows = constructors.flatMap((constructor) => [
  { constructor, name: "the activation", key: ReactivityKey.activation(spaceId) },
  { constructor, name: "the aggregate status", key: ReactivityKey.aggregateStatus }
])

describe("a subscriber that throws on every notification while a foreground takeover fails to build", () => {
  it.effect.each(takeoverRows)(
    "does not lose the background retry of the retired runtime when it subscribes to $name with $constructor",
    Effect.fnUntraced(function*(row) {
      const services = yield* twoSpaces(row.constructor)
      yield* BackgroundReplica.seedPending(services, [spaceId])
      const attempts = yield* makeAttempts
      const replica = yield* services.start(hangingThenDrain(services, attempts))
      const space = yield* replica.space(spaceId)
      yield* VirtualTime.advanceUntil(attempts.reached(1))
      let throws = 0
      services.reactivity.registerUnsafe([row.key], () => {
        throws += 1
        decodeURIComponent("%")
      })
      services.lockNext("SELECT desired_scope_json")

      const activated = yield* within(space.activate)
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      yield* settle("5 minutes")
      const status = yield* space.status

      assert.isAbove(throws, 0, "the subscriber threw")
      assert.strictEqual(describeExit(activated), "failed", "the foreground activation")
      assert.isTrue(Option.isSome(retried), "the background turn of the retired runtime ran again")
      assert.strictEqual(describeStatus(status), "Idle, pending 0")
    }, VirtualTime.scoped)
  )
})
