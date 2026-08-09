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
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as FaultInjection from "./FaultInjection.js"

export class InvalidFault extends Schema.TaggedErrorClass<InvalidFault>(
  "@lucas-barake/effect-local-test/InvalidFault"
)("InvalidFault", {
  sequence: Schema.Int,
  reason: Schema.String
}) {}

export class QueueFull extends Schema.TaggedErrorClass<QueueFull>(
  "@lucas-barake/effect-local-test/QueueFull"
)("QueueFull", {
  from: Identity.PeerId,
  to: Identity.PeerId,
  capacity: Schema.Int
}) {}

export class ConnectionClosed extends Schema.TaggedErrorClass<ConnectionClosed>(
  "@lucas-barake/effect-local-test/ConnectionClosed"
)("ConnectionClosed", {
  from: Identity.PeerId,
  to: Identity.PeerId
}) {}

export class InvalidOptions extends Schema.TaggedErrorClass<InvalidOptions>(
  "@lucas-barake/effect-local-test/InvalidOptions"
)("InvalidOptions", {
  reason: Schema.String
}) {}

export type TestPeerError = InvalidFault | QueueFull | ConnectionClosed

export interface Hooks {
  readonly deliveryStarted?: Effect.Effect<void>
  readonly retiring?: Effect.Effect<void>
}

export interface Options {
  readonly queueCapacity: number
  readonly maxCopies: number
  readonly maxDelay: Duration.Input
  readonly hooks?: Hooks
}

export interface Connection {
  readonly from: Identity.PeerId
  readonly to: Identity.PeerId
  readonly receive: Stream.Stream<Uint8Array>
  readonly send: (message: Uint8Array) => Effect.Effect<void, TestPeerError>
  readonly queued: Effect.Effect<number>
  readonly close: Effect.Effect<void>
}

export class TestPeer extends Context.Service<TestPeer, {
  readonly connect: (
    from: Identity.PeerId,
    to: Identity.PeerId
  ) => Effect.Effect<Connection, never, Scope.Scope>
  readonly partition: (left: Identity.PeerId, right: Identity.PeerId) => Effect.Effect<void>
  readonly heal: (left: Identity.PeerId, right: Identity.PeerId) => Effect.Effect<void>
  readonly flush: Effect.Effect<void, TestPeerError>
  readonly transport: (peerId: Identity.PeerId) => PeerTransport.PeerTransport["Service"]
}>()("@lucas-barake/effect-local-test/TestPeer") {}

interface Scheduled {
  readonly packet: FaultInjection.Packet
  readonly decision: FaultInjection.Decision
  readonly queue: Queue.Queue<ReadonlyArray<Uint8Array>>
  readonly sourceActive: Ref.Ref<boolean>
}

const route = (from: Identity.PeerId, to: Identity.PeerId) => `${from}\u0000${to}`

const fallbackMessageHash = (message: Uint8Array): string =>
  Array.from(message, (byte) => byte.toString(16).padStart(2, "0"))
    .join("")
    .padEnd(64, "0")
    .slice(0, 64)

const deliveryIdentity = (
  message: Uint8Array,
  relayPeerId: Identity.PeerId,
  senderPeerId: Identity.PeerId
): PeerTransport.RelayDeliveryIdentity => {
  let messageHash = fallbackMessageHash(message)
  try {
    const decoded = JSON.parse(new TextDecoder().decode(message)) as { readonly messageHash?: unknown }
    if (typeof decoded.messageHash === "string" && /^[0-9a-f]{64}$/.test(decoded.messageHash)) {
      messageHash = decoded.messageHash
    }
  } catch {
    // Raw packets are supported by the low level test transport.
  }
  const relayMessageId = Identity.RelayMessageId.make(
    `rly_${messageHash.slice(0, 8)}-${messageHash.slice(8, 12)}-4${messageHash.slice(13, 16)}-8${
      messageHash.slice(17, 20)
    }-${messageHash.slice(20, 32)}`
  )
  return {
    relayMessageId,
    relayPeerId,
    senderTenantId: "effect-local-test",
    senderSubjectId: senderPeerId,
    senderPeerId,
    senderReplicaIncarnation: Identity.ReplicaIncarnation.make(0),
    messageHash,
    outerEnvelopeDigest: messageHash
  }
}

const toValidatedMillis = (input: Duration.Input) =>
  typeof input === "number" && Number.isNaN(input)
    ? Number.NaN
    : Duration.toMillis(input)

export const make = (
  options: Options
): Effect.Effect<TestPeer["Service"], InvalidOptions, FaultInjection.FaultInjection> =>
  Effect.gen(function*() {
    const maxDelay = toValidatedMillis(options.maxDelay)
    if (!Number.isSafeInteger(options.queueCapacity) || options.queueCapacity < 1) {
      return yield* new InvalidOptions({ reason: "queueCapacity must be a positive integer" })
    }
    if (!Number.isSafeInteger(options.maxCopies) || options.maxCopies < 1) {
      return yield* new InvalidOptions({ reason: "maxCopies must be a positive integer" })
    }
    if (!Number.isFinite(maxDelay) || maxDelay < 0) {
      return yield* new InvalidOptions({ reason: "maxDelay must be finite and nonnegative" })
    }
    const faults = yield* FaultInjection.FaultInjection
    const routes = yield* SubscriptionRef.make<
      ReadonlyMap<string, {
        readonly active: Ref.Ref<boolean>
        readonly queue: Queue.Queue<ReadonlyArray<Uint8Array>>
      }>
    >(new Map())
    const partitions = yield* Ref.make<ReadonlySet<string>>(new Set())
    const held = yield* Ref.make<ReadonlyMap<string, Scheduled>>(new Map())
    const sequence = yield* Ref.make(0)
    const transientRoutes = yield* Ref.make<
      ReadonlyMap<string, {
        readonly token: symbol
        readonly queue: Queue.Queue<PeerTransport.TransientDelivery>
      }>
    >(new Map())

    const validate = (packet: FaultInjection.Packet, decision: FaultInjection.Decision) => {
      const delay = toValidatedMillis(decision.delay)
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
      yield* options.hooks?.deliveryStarted ?? Effect.void
      yield* Effect.sleep(delay)
      if ((yield* Ref.get(partitions)).has(route(scheduled.packet.from, scheduled.packet.to))) return
      const failure = yield* Effect.sync(() => {
        if (!Ref.getUnsafe(scheduled.sourceActive)) {
          return new ConnectionClosed({ from: scheduled.packet.from, to: scheduled.packet.to })
        }
        const offered = Queue.offerUnsafe(
          scheduled.queue,
          Array.from({ length: scheduled.decision.copies }, () => scheduled.packet.payload.slice())
        )
        if (offered) return null
        return scheduled.queue.state._tag === "Open"
          ? new QueueFull({
            from: scheduled.packet.from,
            to: scheduled.packet.to,
            capacity: options.queueCapacity
          })
          : new ConnectionClosed({ from: scheduled.packet.from, to: scheduled.packet.to })
      })
      if (failure !== null) return yield* failure
    })

    const send = Effect.fnUntraced(function*(
      from: Identity.PeerId,
      to: Identity.PeerId,
      sourceActive: Ref.Ref<boolean>,
      payload: Uint8Array
    ) {
      const packet: FaultInjection.Packet = {
        sequence: yield* Ref.getAndUpdate(sequence, (current) => current + 1),
        from,
        to,
        payload: payload.slice()
      }
      const decision = yield* faults.decide(packet)
      yield* validate(packet, decision)
      const connection = (yield* SubscriptionRef.get(routes)).get(route(from, to))
      if (connection === undefined) return yield* new ConnectionClosed({ from, to })
      if ((yield* Ref.get(partitions)).has(route(from, to)) || decision.drop) {
        if (!Ref.getUnsafe(sourceActive)) return yield* new ConnectionClosed({ from, to })
        return
      }
      const scheduled = { packet, decision, queue: connection.queue, sourceActive }
      const pending = yield* Ref.modify(held, (current): readonly [
        ReadonlyArray<Scheduled> | false | undefined,
        ReadonlyMap<string, Scheduled>
      ] => {
        if (!Ref.getUnsafe(sourceActive)) return [false, current]
        const key = route(from, to)
        const candidate = current.get(key)
        const previous = candidate?.queue === scheduled.queue && candidate.sourceActive === scheduled.sourceActive
          ? candidate
          : undefined
        const next = new Map(current)
        next.delete(key)
        if (decision.reorder && previous === undefined) {
          next.set(key, scheduled)
          return [undefined, next]
        }
        if (previous === undefined) return [[scheduled], next]
        return [[scheduled, previous], next]
      })
      if (pending === false) return yield* new ConnectionClosed({ from, to })
      if (pending === undefined) return
      const [current, ...deferred] = pending
      yield* deliver(current)
      // deferred packets ride a later send; their delivery failure must not fail it
      yield* Effect.forEach(deferred, (packet) => Effect.catchIf(deliver(packet), () => true, () => Effect.void), {
        discard: true
      })
    })

    const flush = Ref.getAndSet(held, new Map()).pipe(
      Effect.flatMap((pending) => Effect.forEach(pending.values(), deliver, { discard: true }))
    )

    const connect = Effect.fnUntraced(function*(from: Identity.PeerId, to: Identity.PeerId) {
      const inbound = yield* Queue.dropping<ReadonlyArray<Uint8Array>>(options.queueCapacity)
      const active = yield* Ref.make(true)
      const close = Ref.getAndSet(active, false).pipe(
        Effect.flatMap((wasActive) =>
          wasActive
            ? Effect.all([
              SubscriptionRef.update(routes, (current) => {
                const key = route(to, from)
                if (current.get(key)?.queue !== inbound) return current
                const next = new Map(current)
                next.delete(key)
                return next
              }),
              Ref.update(held, (current) => {
                const next = new Map(current)
                const inboundKey = route(to, from)
                if (next.get(inboundKey)?.queue === inbound) next.delete(inboundKey)
                const outboundKey = route(from, to)
                if (next.get(outboundKey)?.sourceActive === active) next.delete(outboundKey)
                return next
              }),
              Queue.shutdown(inbound)
            ], { discard: true })
            : Effect.void
        )
      )
      yield* Effect.addFinalizer(() => close)
      yield* Effect.gen(function*() {
        const previous = yield* SubscriptionRef.modify(routes, (current) => {
          const key = route(to, from)
          const next = new Map(current)
          next.set(key, { active, queue: inbound })
          return [current.get(key), next]
        })
        if (previous !== undefined) {
          yield* Ref.set(previous.active, false)
          yield* options.hooks?.retiring ?? Effect.void
          yield* Ref.update(held, (current) => {
            const next = new Map(current)
            const inboundKey = route(to, from)
            if (next.get(inboundKey)?.queue === previous.queue) next.delete(inboundKey)
            const outboundKey = route(from, to)
            if (next.get(outboundKey)?.sourceActive === previous.active) next.delete(outboundKey)
            return next
          })
          yield* Queue.shutdown(previous.queue)
        }
      }).pipe(Effect.uninterruptible)
      return {
        from,
        to,
        receive: Stream.fromQueue(inbound).pipe(Stream.flattenIterable),
        send: (message: Uint8Array) =>
          Ref.get(active).pipe(
            Effect.flatMap((isActive) =>
              isActive ? send(from, to, active, message) : Effect.fail(new ConnectionClosed({ from, to }))
            )
          ),
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

    const service: TestPeer["Service"] = {
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
        // Both ends of this transport are this build, which compares lineage before it merges, so
        // the advertisement is a statement of fact rather than a test convenience.
        capabilities: { lineageAware: true },
        connect: ({ peerId: remotePeerId }) =>
          connect(peerId, remotePeerId).pipe(
            Effect.tap(() =>
              SubscriptionRef.changes(routes).pipe(
                Stream.filter((current) => current.has(route(peerId, remotePeerId))),
                Stream.runHead
              )
            ),
            Effect.flatMap((connection) =>
              Effect.gen(function*() {
                const token = Symbol("test peer transient route")
                const transientInbound = yield* Queue.dropping<PeerTransport.TransientDelivery>(options.queueCapacity)
                yield* Ref.update(transientRoutes, (current) =>
                  new Map(current).set(route(remotePeerId, peerId), { token, queue: transientInbound }))
                const unregisterTransient = Ref.update(transientRoutes, (current) => {
                  const key = route(remotePeerId, peerId)
                  if (current.get(key)?.token !== token) {
                    return current
                  }
                  const next = new Map(current)
                  next.delete(key)
                  return next
                }).pipe(Effect.andThen(Queue.shutdown(transientInbound)))
                yield* Effect.addFinalizer(() =>
                  unregisterTransient
                )
                const close = connection.close.pipe(Effect.ensuring(unregisterTransient))
                return {
                  peerId: remotePeerId,
                  relayPeerId: peerId,
                  capabilities: { lineageAware: true },
                  receive: Stream.merge(
                    connection.receive.pipe(
                      Stream.map((message): PeerTransport.Inbound => ({
                        _tag: "Durable",
                        delivery: {
                          message,
                          identity: deliveryIdentity(message, peerId, remotePeerId),
                          receiptRetentionMillis: 24 * 60 * 60 * 1_000,
                          acknowledge: Effect.void,
                          reject: () => Effect.void
                        }
                      }))
                    ),
                    Stream.fromQueue(transientInbound).pipe(
                      Stream.map((delivery): PeerTransport.Inbound => ({ _tag: "Transient", delivery }))
                    )
                  ),
                  send: (message: Uint8Array) =>
                    connection.send(message).pipe(
                      Effect.mapError((error) =>
                        new ReplicaError.ReplicaError({
                          reason: new ReplicaError.StorageUnavailable({ cause: error })
                        })
                      )
                    ),
                  transient: (documentId: Identity.DocumentId, payload: Uint8Array) =>
                    Effect.sync(() => {
                      if (Ref.getUnsafe(partitions).has(route(peerId, remotePeerId))) return
                      const target = Ref.getUnsafe(transientRoutes).get(route(peerId, remotePeerId))
                      if (target === undefined) return
                      Queue.offerUnsafe(target.queue, { peerId, documentId, payload: payload.slice() })
                    }),
                  close
                }
              })
            )
          )
      })
    }
    return service
  })

export const layer = (options: Options) => Layer.effect(TestPeer, make(options))

export const transportLayer = (peerId: Identity.PeerId) =>
  Layer.effect(
    PeerTransport.PeerTransport,
    TestPeer.pipe(Effect.map((testPeer) => testPeer.transport(peerId)))
  )
