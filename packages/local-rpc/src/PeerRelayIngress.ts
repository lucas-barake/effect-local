import { Cause, Context, Deferred, Effect, Fiber, Layer, Queue, Scope } from "effect"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import type * as RpcMessage from "effect/unstable/rpc/RpcMessage"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import { Socket, SocketServer } from "effect/unstable/socket"
import { PeerRelayLimits } from "./PeerRelayLimits.ts"
import * as PeerRpcError from "./PeerRpcError.ts"

export interface Usage {
  readonly connections: number
  readonly reservedBytes: number
  readonly byteReservationWaiters: number
}

export interface Reservation {
  readonly bytes: number
  readonly release: Effect.Effect<void>
  readonly transferToCurrentRequest: Effect.Effect<void, PeerRpcError.SessionUnavailable>
}

export class PeerRelayIngress extends Context.Service<
  PeerRelayIngress,
  {
    readonly address: SocketServer.Address
    readonly reserveOutbound: (
      bytes: number
    ) => Effect.Effect<
      Reservation,
      PeerRpcError.RequestLimitExceeded | PeerRpcError.RequestCapacityExceeded
    >
    readonly usage: Effect.Effect<Usage>
    readonly await: Effect.Effect<never, SocketServer.SocketServerError>
  }
>()("@lucas-barake/effect-local-rpc/PeerRelayIngress") {}

interface RequestKey {
  readonly clientId: number
  readonly requestId: string | number
}

const CurrentRequestKey = Context.Reference<RequestKey | undefined>(
  "@lucas-barake/effect-local-rpc/PeerRelayIngress/CurrentRequestKey",
  { defaultValue: () => undefined }
)

interface InternalReservation extends Reservation {
  readonly releaseUnsafe: () => void
  readonly shrinkTo: (bytes: number) => Effect.Effect<void>
  readonly transfer: (key: RequestKey) => Effect.Effect<void, PeerRpcError.SessionUnavailable>
}

interface ByteWaiter {
  readonly bytes: number
  readonly deferred: Deferred.Deferred<InternalReservation>
  state: "Waiting" | "Reserved" | "Delivered" | "Cancelled"
}

const makeByteBudget = (
  capacity: number,
  maximumWaiters: number,
  outbound: Map<number, Map<string | number, InternalReservation>>,
  isClientActive: (clientId: number) => boolean = () => true
) => {
  let reservedBytes = 0
  let waiterId = 0
  const waiters = new Map<number, ByteWaiter>()

  const makeReservation = (bytes: number): InternalReservation => {
    let reserved = bytes
    let released = false
    let transferred = false
    const releaseUnsafe = () => {
      if (released) return
      released = true
      reservedBytes -= reserved
      drain()
    }
    const release = Effect.sync(releaseUnsafe)
    const shrinkTo = (target: number) =>
      Effect.sync(() => {
        if (
          released ||
          transferred ||
          !Number.isSafeInteger(target) ||
          target <= 0 ||
          target > reserved
        ) {
          throw new Error("Invalid relay byte reservation shrink")
        }
        if (target === reserved) return
        const releasedBytes = reserved - target
        reserved = target
        reservedBytes -= releasedBytes
        drain()
      })
    const transfer = (key: RequestKey) =>
      Effect.suspend(() => {
        if (released || transferred || !isClientActive(key.clientId)) {
          return Effect.fail(new PeerRpcError.SessionUnavailable())
        }
        let client = outbound.get(key.clientId)
        if (!client) {
          client = new Map()
          outbound.set(key.clientId, client)
        }
        if (client.has(key.requestId)) {
          return Effect.fail(new PeerRpcError.SessionUnavailable())
        }
        client.set(key.requestId, reservation)
        transferred = true
        return Effect.void
      })
    const reservation: InternalReservation = {
      get bytes() {
        return reserved
      },
      release,
      releaseUnsafe,
      shrinkTo,
      transfer,
      transferToCurrentRequest: Effect.flatMap(CurrentRequestKey, (key) =>
        key === undefined
          ? Effect.fail(new PeerRpcError.SessionUnavailable())
          : transfer(key))
    }
    return reservation
  }

  const drain = () => {
    for (const [id, waiter] of waiters) {
      if (waiter.state !== "Waiting") {
        waiters.delete(id)
        continue
      }
      if (reservedBytes + waiter.bytes > capacity) return
      waiters.delete(id)
      waiter.state = "Reserved"
      reservedBytes += waiter.bytes
      Deferred.doneUnsafe(waiter.deferred, Effect.succeed(makeReservation(waiter.bytes)))
    }
  }

  const cancelWaiter = (id: number, waiter: ByteWaiter) =>
    Effect.sync(() => {
      if (waiter.state === "Waiting") {
        waiter.state = "Cancelled"
        waiters.delete(id)
        drain()
      } else if (waiter.state === "Reserved") {
        waiter.state = "Cancelled"
        reservedBytes -= waiter.bytes
        drain()
      }
    })

  const acquire = (
    bytes: number
  ): Effect.Effect<
    InternalReservation,
    PeerRpcError.RequestLimitExceeded | PeerRpcError.RequestCapacityExceeded
  > =>
    Effect.suspend<
      InternalReservation,
      PeerRpcError.RequestLimitExceeded | PeerRpcError.RequestCapacityExceeded,
      never
    >(() => {
      if (!Number.isSafeInteger(bytes) || bytes <= 0 || bytes > capacity) {
        return Effect.fail(new PeerRpcError.RequestLimitExceeded())
      }
      if (waiters.size === 0 && reservedBytes + bytes <= capacity) {
        reservedBytes += bytes
        return Effect.succeed(makeReservation(bytes))
      }
      if (waiters.size >= maximumWaiters) {
        return Effect.fail(new PeerRpcError.RequestCapacityExceeded())
      }
      const id = waiterId++
      const waiter: ByteWaiter = {
        bytes,
        deferred: Deferred.makeUnsafe(),
        state: "Waiting"
      }
      waiters.set(id, waiter)
      return Effect.interruptible(Deferred.await(waiter.deferred)).pipe(
        Effect.onInterrupt(() => cancelWaiter(id, waiter)),
        Effect.tap(() =>
          Effect.sync(() => {
            waiter.state = "Delivered"
          })
        )
      )
    })

  const reserve = (
    bytes: number
  ): Effect.Effect<
    InternalReservation,
    PeerRpcError.RequestLimitExceeded | PeerRpcError.RequestCapacityExceeded
  > =>
    Effect.suspend(() => {
      let acquired: InternalReservation | undefined
      return Effect.uninterruptibleMask(() =>
        acquire(bytes).pipe(
          Effect.tap((reservation) =>
            Effect.sync(() => {
              acquired = reservation
            })
          )
        )
      ).pipe(
        Effect.onInterrupt(() => acquired?.release ?? Effect.void)
      )
    })

  const use = <A, E, R,>(
    bytes: number,
    f: (reservation: InternalReservation) => Effect.Effect<A, E, R>
  ): Effect.Effect<
    A,
    E | PeerRpcError.RequestLimitExceeded | PeerRpcError.RequestCapacityExceeded,
    R
  > =>
    Effect.uninterruptibleMask((restore) =>
      acquire(bytes).pipe(
        Effect.flatMap((reservation) =>
          restore(f(reservation)).pipe(
            Effect.ensuring(reservation.release)
          )
        )
      )
    )

  return {
    reserve,
    use,
    usage: () => ({
      reservedBytes,
      byteReservationWaiters: waiters.size
    })
  }
}

const invalidFrame = () =>
  new Socket.SocketError({
    reason: new Socket.SocketCloseError({ code: 1009 })
  })

const utf8Length = (value: string): number => {
  let bytes = 0
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index)
    if (code < 0x80) {
      bytes++
    } else if (code < 0x800) {
      bytes += 2
    } else if (code >= 0xd800 && code <= 0xdbff && index + 1 < value.length) {
      const next = value.charCodeAt(index + 1)
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4
        index++
      } else {
        bytes += 3
      }
    } else {
      bytes += 3
    }
  }
  return bytes
}

const encodeFrame = (value: unknown, maximumFrameBytes: number) => {
  const body = new TextEncoder().encode(JSON.stringify(value))
  if (body.byteLength === 0 || body.byteLength > maximumFrameBytes) {
    throw invalidFrame()
  }
  const frame = new Uint8Array(4 + body.byteLength)
  new DataView(frame.buffer).setUint32(0, body.byteLength, false)
  frame.set(body, 4)
  return { bodyBytes: body.byteLength, frame }
}

const makeFrameReader = (
  socket: Socket.Socket,
  limits: {
    readonly maximumRawChunkBytes: number
    readonly maximumDeclaredFrameBytes: number
    readonly maximumIncompleteFrameBytes: number
    readonly incompleteFrameTimeoutMillis: number
  },
  reserve: (
    bytes: number
  ) => Effect.Effect<
    InternalReservation,
    PeerRpcError.RequestLimitExceeded | PeerRpcError.RequestCapacityExceeded
  >,
  onFrame: (
    value: unknown,
    reservation: InternalReservation
  ) => Effect.Effect<void, Socket.SocketError>
) =>
  Effect.gen(function*() {
    const scope = yield* Effect.scope
    const timeout = Deferred.makeUnsafe<never, Socket.SocketError>()
    const header = new Uint8Array(4)
    const decoder = new TextDecoder("utf-8", { fatal: true })
    let headerBytes = 0
    let body: Uint8Array | undefined
    let bodyBytes = 0
    let reservation: InternalReservation | undefined
    let timer: Fiber.Fiber<void> | undefined
    let chunkActive = false
    let chunkRejected = false

    const stopTimer = Effect.suspend(() => {
      if (timer === undefined) return Effect.void
      const current = timer
      timer = undefined
      return Fiber.interrupt(current)
    })

    const startTimer = Effect.suspend(() => {
      if (timer !== undefined) return Effect.void
      return Effect.forkIn(
        Effect.sleep(limits.incompleteFrameTimeoutMillis).pipe(
          Effect.andThen(Effect.sync(() => {
            Deferred.doneUnsafe(timeout, Effect.fail(invalidFrame()))
          }))
        ),
        scope
      ).pipe(
        Effect.tap((fiber) =>
          Effect.sync(() => {
            timer = fiber
          })
        ),
        Effect.asVoid
      )
    })

    const resetFrame = () => {
      headerBytes = 0
      body = undefined
      bodyBytes = 0
      reservation = undefined
    }

    const consume = (raw: string | Uint8Array) =>
      Effect.gen(function*() {
        const rawBytes = typeof raw === "string" ? utf8Length(raw) : raw.byteLength
        if (rawBytes === 0) return
        if (rawBytes > limits.maximumRawChunkBytes) {
          return yield* Effect.fail(invalidFrame())
        }
        const chunk = typeof raw === "string" ? new TextEncoder().encode(raw) : raw
        if (chunk.byteLength !== rawBytes) {
          return yield* Effect.fail(invalidFrame())
        }
        let offset = 0
        while (offset < chunk.byteLength) {
          if (headerBytes === 0 && body === undefined) {
            yield* startTimer
          }
          if (headerBytes < 4) {
            const count = Math.min(4 - headerBytes, chunk.byteLength - offset)
            header.set(chunk.subarray(offset, offset + count), headerBytes)
            headerBytes += count
            offset += count
            if (headerBytes < 4) continue

            const declared = new DataView(header.buffer).getUint32(0, false)
            if (
              declared === 0 ||
              declared > limits.maximumDeclaredFrameBytes ||
              declared + 4 > limits.maximumIncompleteFrameBytes
            ) {
              return yield* Effect.fail(invalidFrame())
            }
            reservation = yield* reserve(declared).pipe(
              Effect.mapError(() => invalidFrame())
            )
            body = new Uint8Array(declared)
          }

          const count = Math.min(body!.byteLength - bodyBytes, chunk.byteLength - offset)
          body!.set(chunk.subarray(offset, offset + count), bodyBytes)
          bodyBytes += count
          offset += count
          if (bodyBytes < body!.byteLength) continue

          yield* stopTimer
          const retained = reservation!
          let value: unknown
          try {
            value = JSON.parse(decoder.decode(body!))
          } catch {
            yield* retained.release
            return yield* Effect.fail(invalidFrame())
          }
          resetFrame()
          yield* onFrame(value, retained).pipe(
            Effect.onExit((exit) => exit._tag === "Failure" ? retained.release : Effect.void)
          )
        }
      })

    return yield* socket.runRaw((chunk) => {
      if (chunkRejected) return
      if (chunkActive) {
        chunkRejected = true
        return Effect.fail(invalidFrame())
      }
      chunkActive = true
      return consume(chunk).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            chunkActive = false
          })
        )
      )
    }).pipe(
      Effect.raceFirst(Deferred.await(timeout)),
      Effect.ensuring(
        Effect.andThen(stopTimer, Effect.suspend(() => reservation === undefined ? Effect.void : reservation.release))
      )
    )
  })

const removeReservation = (
  reservations: Map<number, Map<string | number, InternalReservation>>,
  key: RequestKey
) => {
  const client = reservations.get(key.clientId)
  const reservation = client?.get(key.requestId)
  if (reservation === undefined) return undefined
  client!.delete(key.requestId)
  if (client!.size === 0) reservations.delete(key.clientId)
  return reservation
}

const removeAndReleaseReservation = (
  reservations: Map<number, Map<string | number, InternalReservation>>,
  key: RequestKey
) =>
  Effect.sync(() => {
    removeReservation(reservations, key)?.releaseUnsafe()
  })

const releaseClientReservations = (
  reservations: Map<number, Map<string | number, InternalReservation>>,
  clientId: number
) =>
  Effect.sync(() => {
    const client = reservations.get(clientId)
    if (client === undefined) return
    reservations.delete(clientId)
    for (const reservation of client.values()) {
      reservation.releaseUnsafe()
    }
  })

const makeServerProtocol = (
  server: SocketServer.SocketServer["Service"],
  limits: Context.Service.Shape<typeof PeerRelayLimits>,
  childScope: Scope.Closeable
) =>
  Effect.gen(function*() {
    const disconnects = yield* Queue.bounded<number>(limits.maxRelayConnections)
    const clients = new Map<number, {
      readonly write: (
        chunk: Uint8Array | string | Socket.CloseEvent
      ) => Effect.Effect<void, Socket.SocketError>
    }>()
    const clientIds = new Set<number>()
    const inbound = new Map<number, Map<string | number, InternalReservation>>()
    const outbound = new Map<number, Map<string | number, InternalReservation>>()
    const budget = makeByteBudget(
      limits.maximumSharedPayloadBytes,
      limits.maximumByteReservationWaiters,
      outbound,
      (clientId) => clientIds.has(clientId)
    )
    let connections = 0
    let nextClientId = 0
    let protocolRunners = 0
    let writeRequest!: (
      clientId: number,
      message: RpcMessage.FromClientEncoded
    ) => Effect.Effect<void>

    const cleanupClient = (clientId: number) =>
      Effect.gen(function*() {
        clients.delete(clientId)
        const wasRegistered = clientIds.delete(clientId)
        yield* releaseClientReservations(inbound, clientId)
        yield* releaseClientReservations(outbound, clientId)
        if (wasRegistered) {
          yield* Queue.offer(disconnects, clientId)
        }
      })

    const baseProtocol = yield* RpcServer.Protocol.make((writeRequest_) => {
      writeRequest = writeRequest_
      return Effect.succeed({
        disconnects,
        send: (clientId, response) =>
          Effect.uninterruptibleMask((restore) =>
            Effect.flatMap(CurrentRequestKey, (currentRequest) =>
              Effect.sync(() => {
                const key = "requestId" in response
                  ? { clientId, requestId: response.requestId }
                  : response._tag === "Defect"
                  ? currentRequest
                  : undefined
                const transferred = key === undefined
                  ? undefined
                  : removeReservation(outbound, key)
                return { key, transferred }
              })).pipe(
                Effect.flatMap(({ key, transferred }) =>
                  restore(Effect.gen(function*() {
                    const client = clients.get(clientId)
                    if (client === undefined) {
                      return
                    }

                    const maximumAdditional = transferred === undefined
                      ? limits.maximumDeclaredFrameBytes
                      : Math.max(0, limits.maximumDeclaredFrameBytes - transferred.bytes)
                    const sendWithExtra = (extra: InternalReservation | undefined) =>
                      Effect.gen(function*() {
                        let encoded: ReturnType<typeof encodeFrame>
                        try {
                          encoded = encodeFrame(response, limits.maximumDeclaredFrameBytes)
                        } catch {
                          yield* client.write(new Socket.CloseEvent(1009)).pipe(Effect.ignore)
                          return
                        }

                        const additional = transferred === undefined
                          ? encoded.bodyBytes
                          : Math.max(0, encoded.bodyBytes - transferred.bytes)
                        if (extra !== undefined && additional > 0) {
                          yield* extra.shrinkTo(additional)
                        } else if (extra !== undefined) {
                          yield* extra.release
                        }

                        yield* client.write(encoded.frame).pipe(Effect.ignore)

                        if (
                          (response._tag === "Exit" || response._tag === "Defect") &&
                          key !== undefined
                        ) {
                          yield* removeAndReleaseReservation(inbound, key)
                        }
                      })
                    if (maximumAdditional === 0) {
                      yield* sendWithExtra(undefined)
                    } else {
                      yield* budget.use(
                        maximumAdditional,
                        sendWithExtra
                      ).pipe(
                        Effect.catch(() => client.write(new Socket.CloseEvent(1013)).pipe(Effect.ignore))
                      )
                    }
                  })).pipe(Effect.ensuring(transferred?.release ?? Effect.void))
                )
              )
          ),
        end: (clientId) =>
          Effect.gen(function*() {
            const client = clients.get(clientId)
            if (client !== undefined) {
              yield* client.write(new Socket.CloseEvent()).pipe(Effect.ignore)
            }
            yield* cleanupClient(clientId)
          }),
        clientIds: Effect.sync(() => clientIds),
        initialMessage: Effect.succeedNone,
        supportsAck: true,
        supportsTransferables: false,
        supportsSpanPropagation: true
      })
    })
    const protocol: RpcServer.Protocol["Service"] = {
      ...baseProtocol,
      run: (handler) =>
        Effect.acquireUseRelease(
          Effect.sync(() => {
            protocolRunners++
          }),
          () => baseProtocol.run(handler),
          () =>
            Effect.sync(() => {
              protocolRunners--
            })
        )
    }

    const onSocket = (socket: Socket.Socket) =>
      Effect.scoped(
        Effect.suspend(() => {
          if (
            protocolRunners === 0 ||
            connections + Queue.sizeUnsafe(disconnects) >= limits.maxRelayConnections
          ) {
            return Effect.gen(function*() {
              const write = yield* socket.writer
              yield* socket.runRaw(
                () => Effect.void,
                { onOpen: write(new Socket.CloseEvent(1013)).pipe(Effect.ignore) }
              ).pipe(Effect.catch(() => Effect.void))
            })
          }
          connections++
          const clientId = nextClientId++
          return Effect.gen(function*() {
            const write = yield* socket.writer
            clients.set(clientId, { write })
            clientIds.add(clientId)

            yield* makeFrameReader(
              socket,
              limits,
              budget.reserve,
              (value, reservation): Effect.Effect<void, Socket.SocketError> => {
                if (
                  typeof value !== "object" ||
                  value === null ||
                  Array.isArray(value) ||
                  !("_tag" in value)
                ) {
                  return Effect.andThen(reservation.release, Effect.fail(invalidFrame()))
                }
                const tag = value._tag
                if (
                  tag !== "Request" &&
                  tag !== "Ack" &&
                  tag !== "Interrupt" &&
                  tag !== "Ping" &&
                  tag !== "Eof"
                ) {
                  return Effect.andThen(reservation.release, Effect.fail(invalidFrame()))
                }
                const message = value as RpcMessage.FromClientEncoded
                if (message._tag !== "Request") {
                  return Effect.provideService(
                    writeRequest(clientId, message),
                    CurrentRequestKey,
                    undefined
                  ).pipe(Effect.ensuring(reservation.release))
                }
                if (typeof message.id !== "string" && typeof message.id !== "number") {
                  return Effect.andThen(reservation.release, Effect.fail(invalidFrame()))
                }
                let client = inbound.get(clientId)
                if (client === undefined) {
                  client = new Map()
                  inbound.set(clientId, client)
                }
                if (client.has(message.id)) {
                  return Effect.andThen(reservation.release, Effect.fail(invalidFrame()))
                }
                client.set(message.id, reservation)
                const key = { clientId, requestId: message.id }
                return Effect.provideService(
                  writeRequest(clientId, message),
                  CurrentRequestKey,
                  key
                ).pipe(
                  Effect.onExit((exit) => {
                    if (exit._tag === "Success") return Effect.void
                    return removeAndReleaseReservation(inbound, key)
                  })
                )
              }
            ).pipe(Effect.catch(() => Effect.void))
          }).pipe(
            Effect.ensuring(
              Effect.gen(function*() {
                yield* cleanupClient(clientId)
                connections--
              })
            )
          )
        })
      )

    const acceptFiber = yield* Effect.forkIn(server.run(onSocket), childScope)
    const service = PeerRelayIngress.of({
      address: server.address,
      reserveOutbound: budget.reserve,
      usage: Effect.sync(() => ({
        connections,
        ...budget.usage()
      })),
      await: Fiber.join(acceptFiber)
    })
    return { protocol, service }
  })

export const layerProtocolSocketServer = <E, R,>(
  socketLayer: Layer.Layer<SocketServer.SocketServer, E, R>
): Layer.Layer<
  PeerRelayIngress | RpcServer.Protocol,
  E,
  R | PeerRelayLimits
> =>
  Layer.effectContext(
    Effect.gen(function*() {
      const limits = yield* PeerRelayLimits
      const childScope = yield* Effect.acquireRelease(
        Scope.make("sequential"),
        (scope, exit) => Scope.close(scope, exit)
      )
      const socketContext = yield* Layer.buildWithScope(socketLayer, childScope)
      const server = Context.get(socketContext, SocketServer.SocketServer)
      const { protocol, service } = yield* makeServerProtocol(server, limits, childScope)
      return Context.make(PeerRelayIngress, service).pipe(
        Context.add(RpcServer.Protocol, protocol)
      )
    })
  ) as Layer.Layer<PeerRelayIngress | RpcServer.Protocol, E, R | PeerRelayLimits>

const toClientError = (message: string, cause: unknown) =>
  new RpcClientError.RpcClientError({
    reason: new RpcClientError.RpcClientDefect({ message, cause })
  })

export const makeProtocolSocket = Effect.gen(function*() {
  const socket = yield* Socket.Socket
  const limits = yield* PeerRelayLimits
  const outbound = new Map<number, Map<string | number, InternalReservation>>()
  const budget = makeByteBudget(
    limits.maximumSharedPayloadBytes,
    limits.maximumByteReservationWaiters,
    outbound
  )

  return yield* RpcClient.Protocol.make((writeResponse, clientIds) =>
    Effect.gen(function*() {
      const write = yield* socket.writer
      const requestClients = new Map<string | number, number>()
      let currentError: RpcClientError.RpcClientError | undefined

      const read = makeFrameReader(
        socket,
        limits,
        budget.reserve,
        (value, reservation) => {
          if (
            typeof value !== "object" ||
            value === null ||
            Array.isArray(value) ||
            !("_tag" in value)
          ) {
            return Effect.andThen(reservation.release, Effect.fail(invalidFrame()))
          }
          const response = value as RpcMessage.FromServerEncoded
          const requestClient = "requestId" in response
            ? requestClients.get(response.requestId)
            : undefined
          const effect = requestClient === undefined
            ? Effect.forEach(clientIds, (clientId) => writeResponse(clientId, response), {
              discard: true
            })
            : Effect.suspend(() => {
              if (response._tag === "Exit" && "requestId" in response) {
                requestClients.delete(response.requestId)
              }
              return writeResponse(requestClient, response)
            })
          return Effect.ensuring(effect, reservation.release)
        }
      ).pipe(
        Effect.flatMap(() =>
          Effect.fail(
            new Socket.SocketError({
              reason: new Socket.SocketCloseError({ code: 1000 })
            })
          )
        ),
        Effect.tapCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.void
          currentError = toClientError("Relay socket protocol failed", Cause.squash(cause))
          return Effect.forEach(
            clientIds,
            (clientId) =>
              writeResponse(clientId, {
                _tag: "ClientProtocolError",
                error: currentError!
              }),
            { discard: true }
          )
        }),
        Effect.catchCause((cause) => Cause.hasInterruptsOnly(cause) ? Effect.failCause(cause) : Effect.void),
        Effect.forkScoped
      )
      yield* read

      return {
        send: (clientId: number, request: RpcMessage.FromClientEncoded) =>
          Effect.gen(function*() {
            if (currentError !== undefined) return yield* Effect.fail(currentError)
            yield* budget.use(
              limits.maximumDeclaredFrameBytes,
              (reservation) =>
                Effect.gen(function*() {
                  let encoded: ReturnType<typeof encodeFrame>
                  try {
                    encoded = encodeFrame(request, limits.maximumDeclaredFrameBytes)
                  } catch (cause) {
                    return yield* Effect.fail(toClientError("Relay request frame is invalid", cause))
                  }
                  yield* reservation.shrinkTo(encoded.bodyBytes)
                  if (request._tag === "Request") requestClients.set(request.id, clientId)
                  yield* write(encoded.frame).pipe(
                    Effect.mapError((cause) => toClientError("Relay socket write failed", cause))
                  )
                })
            ).pipe(
              Effect.catchTags({
                RequestLimitExceeded: (cause) =>
                  Effect.fail(toClientError("Relay request capacity is exhausted", cause)),
                RequestCapacityExceeded: (cause) =>
                  Effect.fail(toClientError("Relay request capacity is exhausted", cause))
              })
            )
          }),
        supportsAck: true,
        supportsTransferables: false
      }
    })
  )
})

export const layerProtocolSocket: Layer.Layer<
  RpcClient.Protocol,
  never,
  Socket.Socket | PeerRelayLimits
> = Layer.effect(RpcClient.Protocol, makeProtocolSocket)
