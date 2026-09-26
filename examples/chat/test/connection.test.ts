import { type Connection, connectionChanges } from "@effect-local/example-chat-client/connection"
import { spaceId } from "@effect-local/example-chat-shared/domain"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"

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
const unreadable: StatusResult = AsyncResult.fail(new ReplicaError.OwnerUnavailable({ reason: "takeover" }))

const observe = Effect.gen(function*() {
  const statuses = yield* Queue.unbounded<StatusResult>()
  const connections = yield* Queue.unbounded<Connection>()
  yield* connectionChanges(Stream.fromQueue(statuses)).pipe(
    Stream.runForEach((connection) => Queue.offer(connections, connection)),
    Effect.forkScoped
  )
  const untilOnline = Effect.gen(function*() {
    const seen: Array<Connection> = []
    yield* Queue.offer(statuses, online)
    while (seen.at(-1) !== "online") seen.push(yield* Queue.take(connections))
    return seen
  })
  return {
    report: (status: StatusResult) => Queue.offer(statuses, status),
    next: Queue.take(connections),
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
    "does not report offline for an Offline that turns Connecting within the grace",
    Effect.fnUntraced(function*() {
      const connection = yield* observe
      yield* connection.report(offlineStatus)
      assert.strictEqual(yield* connection.next, "connecting")
      yield* TestClock.adjust("1 second")
      yield* connection.report(connectingStatus)
      yield* TestClock.adjust("1 minute")
      assert.notInclude(yield* connection.untilOnline, "offline")
    }, Effect.scoped)
  )

  it.effect(
    "reports offline once the replica stays Offline for the grace",
    Effect.fnUntraced(function*() {
      const connection = yield* observe
      assert.deepStrictEqual(yield* connection.untilOnline, ["online"])
      yield* connection.report(offlineStatus)
      assert.strictEqual(yield* connection.next, "connecting")
      yield* TestClock.adjust("2 seconds")
      assert.strictEqual(yield* connection.next, "offline")
    }, Effect.scoped)
  )

  it.effect(
    "reports offline once the status stays unreadable for the grace",
    Effect.fnUntraced(function*() {
      const connection = yield* observe
      assert.deepStrictEqual(yield* connection.untilOnline, ["online"])
      yield* connection.report(unreadable)
      assert.strictEqual(yield* connection.next, "connecting")
      yield* TestClock.adjust("2 seconds")
      assert.strictEqual(yield* connection.next, "offline")
    }, Effect.scoped)
  )
})
