import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import type * as Socket from "effect/unstable/socket/Socket"
import { literal } from "./internal/literal.js"

/** No relay is part of this topology, so there is no link to be up or down. */
export const NotConfigured = Schema.TaggedStruct("NotConfigured", {})
export type NotConfigured = typeof NotConfigured.Type

/** The socket has not opened yet, or the owning Layer has been torn down. */
export const Disconnected = Schema.TaggedStruct("Disconnected", {})
export type Disconnected = typeof Disconnected.Type

/**
 * The link is down and the client is still trying.
 *
 * `attempts` exists because the retry loop underneath is unbounded, so a device with no network
 * would otherwise sit on this state forever and `Disconnected` would be unreachable in a live
 * stream. Without a counter a consumer cannot tell a dropped packet from an outage. Treat one
 * attempt as "reconnecting" and a growing count as "offline". It resets on every open.
 */
export const Connecting = Schema.TaggedStruct("Connecting", {
  attempts: Schema.Int.check(Schema.isGreaterThan(0))
})
export type Connecting = typeof Connecting.Type

/**
 * The socket is open.
 *
 * This says nothing about any peer. Relay delivery is store and forward, so a message can be
 * accepted while the device it is addressed to has been offline for days.
 */
export const Connected = Schema.TaggedStruct("Connected", {})
export type Connected = typeof Connected.Type

/**
 * Branded so it cannot be confused with `PeerConnectionStatus.Status`, in either direction. They
 * share three tag names and answer different questions, and because every peer session runs over
 * one socket they all go quiet at the same instant the relay drops, which is exactly when a UI is
 * most likely to bind the wrong one. The brand is type level only, so the encoded wire form is a
 * plain tagged union.
 */
export const Status = Schema.Union([NotConfigured, Disconnected, Connecting, Connected]).pipe(
  Schema.brand("@lucas-barake/effect-local-sql/RelayConnectionStatus")
)
export type Status = typeof Status.Type

export const notConfigured: Status = Status.make({ _tag: "NotConfigured" })
export const disconnected: Status = Status.make({ _tag: "Disconnected" })
export const connected: Status = Status.make({ _tag: "Connected" })
export const connecting = (attempts: number): Status => Status.make({ _tag: "Connecting", attempts })

export class RelayConnectionStatus extends Context.Service<RelayConnectionStatus, {
  readonly status: Stream.Stream<Status, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/RelayConnectionStatus") {}

interface State {
  readonly status: Status
  readonly attempts: number
  readonly closed: boolean
}

/**
 * Consecutive `Connecting` values differ by their attempt count, so they are deliberately not
 * collapsed. That is one frame per retry, which the backoff caps at one per five seconds.
 */
const sameStatus = (left: Status, right: Status): boolean => {
  if (left._tag !== right._tag) return false
  if (left._tag === "Connecting" && right._tag === "Connecting") return left.attempts === right.attempts
  return true
}

const makeOwner = Effect.gen(function*() {
  const state = yield* Ref.make<State>({ status: disconnected, attempts: 0, closed: false })
  // The write lock is not for the two hooks, which Effect runs on one fiber and cannot interleave.
  // It is for the third writer: the Layer finalizer runs on the fiber that closes the scope, and
  // without the lock it can lose an update against a hook that is mid flight, leaving the Ref and
  // the published value permanently disagreeing so the next real change dedupes itself away.
  const writes = yield* Semaphore.make(1)
  // Capacity 2, not 1. `PubSub.sliding` picks `BoundedPubSubSingle` at capacity 1, which keeps one
  // shared subscriber counter and one value slot, so a subscriber that falls behind can consume
  // the slot twice and strand another subscriber on a stale value until the next transition. With
  // `Connected` being a steady state that can last a whole session, that leaves a second tab
  // reporting the wrong thing indefinitely. Any capacity above 1 selects a ring buffer with per
  // entry subscriber counts. Latest wins is unaffected: the buffer still drops the oldest, the
  // replay window is still 1, and the reader still dedupes.
  const statuses = yield* Effect.acquireRelease(
    PubSub.sliding<Status>({ capacity: 2, replay: 1 }),
    (pubsub) =>
      writes.withPermit(
        Effect.gen(function*() {
          yield* Ref.update(state, (current) => ({ ...current, closed: true }))
          yield* PubSub.shutdown(pubsub)
        })
      ).pipe(Effect.uninterruptible)
  )
  // Seeds the replay window. Without it the first subscriber blocks with nothing until the socket
  // first opens or first fails, which on a healthy long lived socket is forever.
  yield* PubSub.publish(statuses, disconnected)

  const transition = (next: (attempts: number) => readonly [Status, number]) =>
    writes.withPermit(
      Effect.gen(function*() {
        const current = yield* Ref.get(state)
        if (current.closed) return
        const [status, attempts] = next(current.attempts)
        if (sameStatus(current.status, status)) return
        yield* Ref.set(state, { status, attempts, closed: false })
        yield* PubSub.publish(statuses, status)
      })
      // `onDisconnect` already runs uninterruptible because Effect.ensuring gives its finalizer an
      // uninterruptible region, but `onConnect` is yielded inline inside the socket's own run loop
      // with no mask, so an interrupt between the write and the publish would strand them apart.
    ).pipe(Effect.uninterruptible)

  const hooks = RpcClient.ConnectionHooks.of({
    onConnect: transition(() => [connected, 0]),
    // Not `Disconnected`. This fires once per terminated attempt and the retry loop is always still
    // running behind it, so claiming the client has given up would be a lie.
    onDisconnect: transition((attempts) => [connecting(attempts + 1), attempts + 1])
  })

  const reader = RelayConnectionStatus.of({
    // The terminal value is appended after the PubSub ends rather than published from the finalizer.
    // `PubSub.shutdown` sets each subscription's shutdown flag before `takeAll` drains its buffer, so
    // a published final value is dropped or delivered depending on scheduling.
    status: Stream.fromPubSub(statuses).pipe(
      Stream.concat(Stream.make(disconnected)),
      Stream.changesWith(sameStatus)
    )
  })

  /**
   * Retires the owner without shutting the PubSub down.
   *
   * The socket fiber is forked into the same Layer scope after the PubSub is acquired, so on close
   * it is interrupted first and its `Effect.ensuring(onDisconnect)` fires while the owner is still
   * accepting writes. Without this, a clean shutdown publishes one last `Connecting` claiming the
   * client is still retrying something that has already been torn down.
   */
  const retire = writes.withPermit(
    Ref.update(state, (current) => ({ ...current, closed: true }))
  ).pipe(Effect.uninterruptible)

  return literal({ hooks, reader, retire })
})

/**
 * Builds the relay socket protocol and its status reader together.
 *
 * They are one Layer because `RpcClient.ConnectionHooks` is read with `Effect.serviceOption` and so
 * never appears in `layerProtocolSocket`'s requirements. Provided separately, three compositions are
 * possible and two of them are silently wrong: `Layer.provide` hides the reader from everything
 * above it, and `Layer.merge` never shows the hooks to the protocol, which compiles and then reports
 * a permanent `Disconnected`. Co-constructing removes the choice, and it keeps the hooks out of every
 * consumer context, which matters because the worker protocol reads the same key and would assert a
 * connection it never retracts.
 *
 * It also ties the owner's lifetime to one socket, so the reported state is a property of that
 * socket rather than of whichever protocol wrote last.
 */
export const layerProtocolSocket = (options?: {
  readonly retryTransientErrors?: boolean | undefined
}): Layer.Layer<
  RpcClient.Protocol | RelayConnectionStatus,
  never,
  Socket.Socket | RpcSerialization.RpcSerialization
> =>
  Layer.effectContext(
    Effect.gen(function*() {
      const owner = yield* makeOwner
      const protocol = yield* RpcClient.makeProtocolSocket(options).pipe(
        Effect.provideService(RpcClient.ConnectionHooks, owner.hooks)
      )
      // Registered after the socket is forked, so LIFO runs it first and the hooks are already
      // retired by the time the socket fiber is interrupted.
      yield* Effect.addFinalizer(() => owner.retire)
      return Context.make(RpcClient.Protocol, protocol).pipe(
        Context.add(RelayConnectionStatus, owner.reader)
      )
    })
  )

/**
 * For a replica with no relay in its topology.
 *
 * Reporting `Disconnected` there would claim a link exists and is down. The stream never ends,
 * because a status stream that completes is indistinguishable from one that is open with nothing to
 * say, and an observer would park on the last value it saw.
 */
export const layerNotConfigured: Layer.Layer<RelayConnectionStatus> = Layer.succeed(
  RelayConnectionStatus,
  RelayConnectionStatus.of({
    status: Stream.make(notConfigured).pipe(Stream.concat(Stream.never))
  })
)
