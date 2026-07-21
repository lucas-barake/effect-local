import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as FaultInjection from "./FaultInjection.js"

export class InvalidFault extends Schema.TaggedErrorClass<InvalidFault>()("TestPeerInvalidFault", {
  sequence: Schema.Int,
  reason: Schema.String
}) {}

export class QueueFull extends Schema.TaggedErrorClass<QueueFull>()("TestPeerQueueFull", {
  from: Identity.PeerId,
  to: Identity.PeerId,
  capacity: Schema.Int
}) {}

export class ConnectionClosed extends Schema.TaggedErrorClass<ConnectionClosed>()("TestPeerConnectionClosed", {
  from: Identity.PeerId,
  to: Identity.PeerId
}) {}

export type TestPeerError = InvalidFault | QueueFull | ConnectionClosed

export interface Options {
  readonly queueCapacity: number
  readonly maxCopies: number
  readonly maxDelay: Duration.Input
}

export interface Connection {
  readonly from: Identity.PeerId
  readonly to: Identity.PeerId
  readonly receive: Stream.Stream<Uint8Array>
  readonly send: (message: Uint8Array) => Effect.Effect<void, TestPeerError>
  readonly queued: Effect.Effect<number>
  readonly close: Effect.Effect<void>
}

export interface Service {
  readonly connect: (
    from: Identity.PeerId,
    to: Identity.PeerId
  ) => Effect.Effect<Connection, never, Scope.Scope>
  readonly partition: (left: Identity.PeerId, right: Identity.PeerId) => Effect.Effect<void>
  readonly heal: (left: Identity.PeerId, right: Identity.PeerId) => Effect.Effect<void>
  readonly flush: Effect.Effect<void, TestPeerError>
  readonly transport: (peerId: Identity.PeerId) => PeerTransport.PeerTransport["Service"]
}

interface Scheduled {
  readonly packet: FaultInjection.Packet
  readonly decision: FaultInjection.Decision
}

export class TestPeer extends Context.Service<TestPeer, Service>()("@lucas-barake/effect-local-test/TestPeer") {}

const route = (from: Identity.PeerId, to: Identity.PeerId) => `${from}\u0000${to}`

export const make = (options: Options): Effect.Effect<Service, never, FaultInjection.FaultInjection> => {
  const maxDelay = Duration.toMillis(options.maxDelay)
  if (!Number.isSafeInteger(options.queueCapacity) || options.queueCapacity < 1) {
    throw new TypeError("TestPeer queueCapacity must be a positive integer")
  }
  if (!Number.isSafeInteger(options.maxCopies) || options.maxCopies < 1) {
    throw new TypeError("TestPeer maxCopies must be a positive integer")
  }
  if (!Number.isFinite(maxDelay) || maxDelay < 0) {
    throw new TypeError("TestPeer maxDelay must be finite and nonnegative")
  }
  return Effect.gen(function*() {
    const faults = yield* FaultInjection.FaultInjection
    const routes = yield* Ref.make<ReadonlyMap<string, Queue.Queue<ReadonlyArray<Uint8Array>>>>(new Map())
    const partitions = yield* Ref.make<ReadonlySet<string>>(new Set())
    const held = yield* Ref.make<ReadonlyMap<string, Scheduled>>(new Map())
    const sequence = yield* Ref.make(0)

    const queueFor = Effect.fnUntraced(function*(from: Identity.PeerId, to: Identity.PeerId) {
      const key = route(from, to)
      const existing = (yield* Ref.get(routes)).get(key)
      if (existing !== undefined) return existing
      const candidate = yield* Queue.dropping<ReadonlyArray<Uint8Array>>(options.queueCapacity)
      return yield* Ref.modify(routes, (current) => {
        const currentQueue = current.get(key)
        if (currentQueue !== undefined) return [currentQueue, current]
        const next = new Map(current)
        next.set(key, candidate)
        return [candidate, next]
      })
    })

    const validate = (packet: FaultInjection.Packet, decision: FaultInjection.Decision) => {
      const delay = Duration.toMillis(decision.delay)
      if (!Number.isSafeInteger(decision.copies) || decision.copies < 1 || decision.copies > options.maxCopies) {
        return Effect.fail(
          new InvalidFault({
            sequence: packet.sequence,
            reason: `copies must be between 1 and ${options.maxCopies}`
          })
        )
      }
      if (!Number.isFinite(delay) || delay < 0 || delay > maxDelay) {
        return Effect.fail(
          new InvalidFault({
            sequence: packet.sequence,
            reason: `delay must be between 0 and ${maxDelay} milliseconds`
          })
        )
      }
      return Effect.succeed(delay)
    }

    const deliver = Effect.fnUntraced(function*(scheduled: Scheduled) {
      const delay = yield* validate(scheduled.packet, scheduled.decision)
      yield* Effect.sleep(delay)
      if ((yield* Ref.get(partitions)).has(route(scheduled.packet.from, scheduled.packet.to))) return
      const queue = yield* queueFor(scheduled.packet.from, scheduled.packet.to)
      const offered = yield* Queue.offer(
        queue,
        Array.from({ length: scheduled.decision.copies }, () => scheduled.packet.payload.slice())
      )
      if (offered) return
      const size = yield* Queue.size(queue)
      return yield* size >= options.queueCapacity
        ? new QueueFull({
          from: scheduled.packet.from,
          to: scheduled.packet.to,
          capacity: options.queueCapacity
        })
        : new ConnectionClosed({ from: scheduled.packet.from, to: scheduled.packet.to })
    })

    const send = Effect.fnUntraced(function*(from: Identity.PeerId, to: Identity.PeerId, payload: Uint8Array) {
      const packet: FaultInjection.Packet = {
        sequence: yield* Ref.getAndUpdate(sequence, (current) => current + 1),
        from,
        to,
        payload: payload.slice()
      }
      const decision = yield* faults.decide(packet)
      yield* validate(packet, decision)
      if ((yield* Ref.get(partitions)).has(route(from, to)) || decision.drop) return
      const scheduled = { packet, decision }
      const pending = yield* Ref.modify(held, (current): readonly [
        ReadonlyArray<Scheduled> | undefined,
        ReadonlyMap<string, Scheduled>
      ] => {
        const key = route(from, to)
        const previous = current.get(key)
        if (decision.reorder && previous === undefined) {
          const next = new Map(current)
          next.set(key, scheduled)
          return [undefined, next]
        }
        if (previous === undefined) return [[scheduled], current]
        const next = new Map(current)
        next.delete(key)
        return [[scheduled, previous], next]
      })
      if (pending === undefined) return
      yield* Effect.forEach(pending, deliver, { discard: true })
    })

    const flush = Ref.getAndSet(held, new Map()).pipe(
      Effect.flatMap((pending) => Effect.forEach(pending.values(), deliver, { discard: true }))
    )

    const connect = Effect.fnUntraced(function*(from: Identity.PeerId, to: Identity.PeerId) {
      const inbound = yield* queueFor(to, from)
      const close = Effect.all([
        Ref.update(routes, (current) => {
          const key = route(to, from)
          if (current.get(key) !== inbound) return current
          const next = new Map(current)
          next.delete(key)
          return next
        }),
        Queue.shutdown(inbound)
      ], { discard: true })
      yield* Effect.addFinalizer(() => close)
      return {
        from,
        to,
        receive: Stream.fromQueue(inbound).pipe(Stream.flatMap(Stream.fromIterable)),
        send: (message: Uint8Array) => send(from, to, message),
        queued: Queue.size(inbound),
        close
      }
    })

    const updatePartition = (left: Identity.PeerId, right: Identity.PeerId, partitioned: boolean) =>
      Ref.update(partitions, (current) => {
        const next = new Set(current)
        for (const key of [route(left, right), route(right, left)]) {
          if (partitioned) next.add(key)
          else next.delete(key)
        }
        return next
      })

    const service: Service = {
      connect,
      partition: (left, right) =>
        Effect.all([
          updatePartition(left, right, true),
          Ref.update(held, (current) => {
            const next = new Map(current)
            next.delete(route(left, right))
            next.delete(route(right, left))
            return next
          })
        ], { discard: true }),
      heal: (left, right) => updatePartition(left, right, false),
      flush,
      transport: (peerId) => ({
        capabilities: { storeAndForward: false },
        connect: ({ peerId: remotePeerId }) =>
          connect(peerId, remotePeerId).pipe(
            Effect.map((connection) => ({
              peerId: remotePeerId,
              capabilities: { storeAndForward: false },
              receive: connection.receive,
              send: (message) =>
                connection.send(message).pipe(
                  Effect.mapError((error) =>
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "StorageUnavailable",
                        cause: {
                          _tag: "RpcCause",
                          message: `${error._tag}: ${"reason" in error ? error.reason : "route unavailable"}`
                        }
                      }
                    })
                  )
                ),
              close: connection.close
            }))
          )
      })
    }
    return service
  })
}

export const layer = (options: Options) => Layer.effect(TestPeer, make(options))

export const transportLayer = (peerId: Identity.PeerId) =>
  Layer.effect(
    PeerTransport.PeerTransport,
    TestPeer.pipe(Effect.map((testPeer) => testPeer.transport(peerId)))
  )
