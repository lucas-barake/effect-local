import { type Connection, connectionChanges } from "@effect-local/example-chat-shared/connection"
import { spaceId } from "@effect-local/example-chat-shared/domain"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Channel from "effect/Channel"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import * as Scheduler from "effect/Scheduler"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"

type StatusResult = AsyncResult.AsyncResult<ReplicaStatus.SpaceStatus, ReplicaError.ReplicaError>

const online: StatusResult = AsyncResult.success({
  spaceId,
  synced: true,
  _tag: "Online",
  pending: 0,
  cursor: Identity.ServerSequence.make(0)
})
const connectingStatus: StatusResult = AsyncResult.success({ spaceId, synced: true, _tag: "Connecting", pending: 0 })
const offlineStatus: StatusResult = AsyncResult.success({ spaceId, synced: true, _tag: "Offline", pending: 0 })
const idleStatus: StatusResult = AsyncResult.success({ spaceId, synced: true, _tag: "Idle", pending: 1 })
const unreadable: StatusResult = AsyncResult.fail(new ReplicaError.OwnerUnavailable({ reason: "takeover" }))
const superseded: StatusResult = AsyncResult.fail(
  new ReplicaError.BuildSuperseded({ version: 1, supersedingVersion: 2 })
)

const withoutYield = Effect.provideService(Scheduler.PreventSchedulerYield, true)

const observe = Effect.gen(function*() {
  const statuses = yield* Queue.unbounded<StatusResult>()
  const connections = yield* Queue.unbounded<Connection>()
  const statusStream = Queue.takeAll(statuses).pipe(withoutYield, Effect.succeed, Channel.fromPull, Stream.fromChannel)
  const nextConnection = withoutYield(Queue.take(connections))
  yield* connectionChanges(statusStream).pipe(
    Stream.runForEach((connection) => Queue.offer(connections, connection)),
    Effect.forkScoped
  )
  const untilOnline = Effect.gen(function*() {
    const seen: Array<Connection> = []
    yield* Queue.offer(statuses, online)
    while (seen.at(-1) !== "online") seen.push(yield* nextConnection)
    return seen
  })
  return {
    report: (status: StatusResult) => Queue.offer(statuses, status),
    next: nextConnection,
    untilOnline
  }
})

describe("chat connection", () => {
  it.effect(
    "keeps a replica that stays Connecting out of the offline state",
    Effect.fnUntraced(function*() {
      const connection = yield* observe
      assert.deepStrictEqual(yield* connection.untilOnline, ["online"])
      yield* connection.report(connectingStatus)
      assert.strictEqual(yield* connection.next, "connecting")
      yield* TestClock.adjust("1 minute")
      assert.deepStrictEqual(yield* connection.untilOnline, ["online"])
    }, Effect.scoped)
  )

  it.effect(
    "keeps a remembered space that is not being synchronized out of the offline state",
    Effect.fnUntraced(function*() {
      const connection = yield* observe
      assert.deepStrictEqual(yield* connection.untilOnline, ["online"])
      yield* connection.report(idleStatus)
      assert.strictEqual(yield* connection.next, "idle")
      yield* TestClock.adjust("1 minute")
      assert.deepStrictEqual(yield* connection.untilOnline, ["online"])
    }, Effect.scoped)
  )

  it.effect(
    "reports offline as soon as the replica reports Offline",
    Effect.fnUntraced(function*() {
      const connection = yield* observe
      assert.deepStrictEqual(yield* connection.untilOnline, ["online"])
      yield* connection.report(offlineStatus)
      assert.strictEqual(yield* connection.next, "offline")
    }, Effect.scoped)
  )

  it.effect(
    "reports an unreadable status as failed, not offline",
    Effect.fnUntraced(function*() {
      const connection = yield* observe
      assert.deepStrictEqual(yield* connection.untilOnline, ["online"])
      yield* connection.report(unreadable)
      assert.strictEqual(yield* connection.next, "failed")
    }, Effect.scoped)
  )

  it.effect(
    "reports a tab whose build another tab superseded as superseded, not failed",
    Effect.fnUntraced(function*() {
      const connection = yield* observe
      assert.deepStrictEqual(yield* connection.untilOnline, ["online"])
      yield* connection.report(superseded)
      assert.strictEqual(yield* connection.next, "superseded")
    }, Effect.scoped)
  )
})
