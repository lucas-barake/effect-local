import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import { RpcClientDefect, RpcClientError } from "effect/unstable/rpc/RpcClientError"
import type { FromClientEncoded, FromServerEncoded } from "effect/unstable/rpc/RpcMessage"
import { constPing } from "effect/unstable/rpc/RpcMessage"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as Wire from "./multiTabWire.js"
import type * as platform from "./platform.js"

const decodeEnvelope = Schema.decodeUnknownEffect(Wire.Envelope)
const decodeVersion = Schema.decodeUnknownEffect(Wire.VersionProbe)

export const postFrame = (
  connection: platform.TabChannelConnection,
  frame: Wire.WireFrame
): Effect.Effect<void> => connection.post({ v: Wire.protocolVersion, frame })

export interface SubscribeOptions {
  readonly onFrame: (frame: Wire.WireFrame) => Effect.Effect<void>
  readonly onVersionSkew?: (version: number) => Effect.Effect<void>
}

export const subscribeFrames = Effect.fnUntraced(function*(
  connection: platform.TabChannelConnection,
  options: SubscribeOptions
) {
  const queue = yield* connection.messages
  const reportSkew = (version: number): Effect.Effect<void> => {
    if (options.onVersionSkew === undefined) return Effect.void
    return options.onVersionSkew(version)
  }
  yield* Queue.take(queue).pipe(
    Effect.flatMap((raw) =>
      decodeEnvelope(raw).pipe(
        Effect.flatMap((envelope) => {
          if (envelope.v === Wire.protocolVersion) return options.onFrame(envelope.frame)
          return reportSkew(envelope.v)
        }),
        Effect.catchTag("SchemaError", () =>
          decodeVersion(raw).pipe(
            Effect.flatMap((probe) => {
              if (probe.v === Wire.protocolVersion) return Effect.void
              return reportSkew(probe.v)
            }),
            Effect.catchTag("SchemaError", () => Effect.void)
          ))
      )
    ),
    Effect.forever,
    Effect.forkScoped
  )
})

export interface ClientProtocolOptions {
  readonly tabId: Wire.TabId
  readonly fingerprint: string
  readonly connection: platform.TabChannelConnection
  readonly heartbeatInterval: Duration.Input
  readonly pingInterval: Duration.Input
  readonly pingTimeout: Duration.Input
  readonly onLeaderSilent?: Effect.Effect<void>
  readonly onLeaderReady?: Effect.Effect<void>
  readonly isParked?: () => boolean
  readonly parkInterrupt?: Effect.Effect<void>
  readonly onIncompatibleLeader?: Effect.Effect<void>
  readonly onVersionSkew?: (version: number) => Effect.Effect<void>
}

export const makeClientProtocol = (
  options: ClientProtocolOptions
): Effect.Effect<RpcClient.Protocol["Service"], never, Scope.Scope> =>
  RpcClient.Protocol.make(Effect.fnUntraced(function*(writeResponse, clientIds) {
    const protocolScope = yield* Effect.scope
    const heartbeatMillis = Duration.toMillis(options.heartbeatInterval)
    const pingMillis = Duration.toMillis(options.pingInterval)
    const pingTimeoutMillis = Duration.toMillis(options.pingTimeout)
    const requestClientMap = new Map<string | number, number>()

    let current: Wire.Ready | undefined
    let highestEpoch = 0
    let incompatible = false
    let awaitingPong = false
    let pongDeadline = 0
    let ready = yield* Deferred.make<void>()

    const failInflight = (message: string) =>
      Effect.suspend(() => {
        requestClientMap.clear()
        const error: FromServerEncoded = {
          _tag: "ClientProtocolError",
          error: new RpcClientError({
            reason: new RpcClientDefect({ message, cause: undefined })
          })
        }
        return Effect.forEach(clientIds, (clientId) => writeResponse(clientId, error))
      })

    const dropLeader = Effect.fnUntraced(function*(message: string) {
      if (current === undefined) return
      current = undefined
      awaitingPong = false
      ready = yield* Deferred.make<void>()
      yield* failInflight(message)
    })

    const adoptLeader = Effect.fnUntraced(function*(frame: Wire.Ready) {
      if (frame.epoch < highestEpoch) return
      highestEpoch = frame.epoch
      if (frame.fingerprint !== options.fingerprint) {
        incompatible = true
        yield* dropLeader("leader definition mismatch")
        yield* failInflight("leader definition mismatch")
        if (options.onIncompatibleLeader !== undefined) yield* options.onIncompatibleLeader
        return
      }
      incompatible = false
      const changed = current === undefined || frame.epoch > current.epoch
      if (current !== undefined && changed) {
        yield* failInflight("leader changed")
      }
      current = frame
      if (changed) awaitingPong = false
      if (changed) {
        yield* postFrame(options.connection, Wire.ClientHello.make({ epoch: frame.epoch, tabId: options.tabId }))
      }
      yield* Deferred.succeed(ready, undefined)
      if (changed && options.onLeaderReady !== undefined) {
        yield* Effect.forkIn(options.onLeaderReady, protocolScope)
      }
    })

    const onFrame = (frame: Wire.WireFrame): Effect.Effect<void> => {
      switch (frame._tag) {
        case "Ready": {
          return adoptLeader(frame)
        }
        case "Elected": {
          return Effect.suspend(() => {
            if (frame.epoch <= highestEpoch) return Effect.void
            highestEpoch = frame.epoch
            return dropLeader("leader takeover in progress")
          })
        }
        case "RpcResponse": {
          return Effect.suspend(() => {
            if (frame.to !== options.tabId) return Effect.void
            if (current === undefined || frame.epoch !== current.epoch) return Effect.void
            // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The envelope schema leaves rpc payloads opaque; the rpc client decodes them against its own request schemas.
            const response = frame.message as FromServerEncoded
            if (response._tag === "Pong") {
              awaitingPong = false
              return Effect.void
            }
            if (Object.hasOwn(response, "requestId")) {
              // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- Narrowed by the hasOwn check on the line above.
              const requestId = (response as FromServerEncoded & { readonly requestId: string | number }).requestId
              const clientId = requestClientMap.get(requestId)
              if (clientId !== undefined) {
                if (response._tag === "Exit") requestClientMap.delete(requestId)
                return writeResponse(clientId, response)
              }
            }
            return Effect.forEach(clientIds, (clientId) => writeResponse(clientId, response))
          })
        }
        default: {
          return Effect.void
        }
      }
    }

    let subscribeOptions: SubscribeOptions = { onFrame }
    if (options.onVersionSkew !== undefined) {
      subscribeOptions = { onFrame, onVersionSkew: options.onVersionSkew }
    }
    yield* subscribeFrames(options.connection, subscribeOptions)

    yield* postFrame(options.connection, Wire.ProbeLeader.make({ tabId: options.tabId }))

    const heartbeat = Effect.suspend(() => {
      if (current === undefined) return Effect.void
      return postFrame(
        options.connection,
        Wire.ClientHeartbeat.make({ epoch: current.epoch, tabId: options.tabId })
      )
    })
    yield* Effect.sleep(heartbeatMillis).pipe(
      Effect.andThen(heartbeat),
      Effect.forever,
      Effect.forkScoped
    )

    const pingTick = Effect.fnUntraced(function*() {
      const leader = current
      if (leader === undefined) return
      const now = yield* Clock.currentTimeMillis
      if (awaitingPong && now >= pongDeadline) {
        yield* dropLeader("leader unresponsive")
        yield* postFrame(options.connection, Wire.ProbeLeader.make({ tabId: options.tabId }))
        if (options.onLeaderSilent !== undefined) yield* options.onLeaderSilent
        return
      }
      if (!awaitingPong) {
        awaitingPong = true
        pongDeadline = now + pingTimeoutMillis
      }
      yield* postFrame(
        options.connection,
        Wire.RpcRequest.make({ epoch: leader.epoch, from: options.tabId, message: constPing })
      )
    })
    yield* Effect.sleep(pingMillis).pipe(
      Effect.andThen(pingTick()),
      Effect.forever,
      Effect.forkScoped
    )

    yield* Effect.addFinalizer(() => postFrame(options.connection, Wire.ClientBye.make({ tabId: options.tabId })))

    const awaitReadyLoop: Effect.Effect<Wire.Ready> = Effect.suspend(() => {
      const leader = current
      if (leader !== undefined) return Effect.succeed(leader)
      return Deferred.await(ready).pipe(Effect.andThen(awaitReadyLoop))
    })

    const parkedError = () =>
      new RpcClientError({
        reason: new RpcClientDefect({
          message: "remote rpc is parked while this tab owns the replica",
          cause: undefined
        })
      })

    const awaitReady = (): Effect.Effect<Wire.Ready, RpcClientError> => {
      if (options.isParked?.() === true) return Effect.fail(parkedError())
      if (options.parkInterrupt === undefined) return awaitReadyLoop
      const failParked = Effect.fail(parkedError())
      return Effect.raceFirst(
        options.parkInterrupt.pipe(Effect.andThen(failParked)),
        awaitReadyLoop
      )
    }

    return {
      send(clientId, request) {
        if (incompatible) {
          return Effect.fail(
            new RpcClientError({
              reason: new RpcClientDefect({ message: "leader definition mismatch", cause: undefined })
            })
          )
        }
        if (request._tag === "Request") {
          requestClientMap.set(request.id, clientId)
        }
        return Effect.flatMap(awaitReady(), (leader) =>
          postFrame(
            options.connection,
            Wire.RpcRequest.make({ epoch: leader.epoch, from: options.tabId, message: request })
          ))
      },
      supportsAck: true,
      supportsTransferables: false
    }
  }))

export interface ServerProtocolOptions {
  readonly epoch: Wire.Epoch
  readonly leaderId: Wire.TabId
  readonly fingerprint: string
  readonly connection: platform.TabChannelConnection
  readonly clientTimeout: Duration.Input
  readonly sweepInterval: Duration.Input
  readonly onVersionSkew?: (version: number) => Effect.Effect<void>
}

export interface ServerProtocol {
  readonly protocol: RpcServer.Protocol["Service"]
  readonly tabIdOf: (clientId: number) => Wire.TabId | undefined
  readonly isConnected: (tabId: Wire.TabId) => boolean
  readonly tabDisconnects: Queue.Dequeue<Wire.TabId>
}

export const makeServerProtocol = Effect.fnUntraced(function*(
  options: ServerProtocolOptions
) {
  const clientTimeoutMillis = Duration.toMillis(options.clientTimeout)
  const sweepMillis = Duration.toMillis(options.sweepInterval)
  const byTabId = new Map<Wire.TabId, { readonly clientId: number; lastSeen: number }>()
  const byClientId = new Map<number, Wire.TabId>()
  let nextClientId = 1
  const disconnects = yield* Queue.make<number>()
  const tabDisconnects = yield* Queue.make<Wire.TabId>()
  const clientIds = new Set<number>()

  let writeRequest: (clientId: number, message: FromClientEncoded) => Effect.Effect<void> = () => Effect.void

  const announceReady = postFrame(
    options.connection,
    Wire.Ready.make({ epoch: options.epoch, leaderId: options.leaderId, fingerprint: options.fingerprint })
  )

  const evict = (tabId: Wire.TabId) =>
    Effect.suspend(() => {
      const entry = byTabId.get(tabId)
      if (entry === undefined) return Effect.void
      byTabId.delete(tabId)
      byClientId.delete(entry.clientId)
      clientIds.delete(entry.clientId)
      return Queue.offer(disconnects, entry.clientId).pipe(
        Effect.andThen(Queue.offer(tabDisconnects, tabId))
      )
    })

  const register = Effect.fnUntraced(function*(tabId: Wire.TabId) {
    const now = yield* Clock.currentTimeMillis
    const existing = byTabId.get(tabId)
    if (existing !== undefined) {
      existing.lastSeen = now
      return existing.clientId
    }
    const clientId = nextClientId++
    byTabId.set(tabId, { clientId, lastSeen: now })
    byClientId.set(clientId, tabId)
    clientIds.add(clientId)
    return clientId
  })

  const onFrame = (frame: Wire.WireFrame): Effect.Effect<void> => {
    switch (frame._tag) {
      case "ClientHello": {
        if (frame.epoch !== options.epoch) return Effect.void
        return register(frame.tabId).pipe(Effect.andThen(announceReady))
      }
      case "ClientHeartbeat": {
        if (frame.epoch !== options.epoch) return Effect.void
        return register(frame.tabId).pipe(Effect.asVoid)
      }
      case "ClientBye": {
        return evict(frame.tabId)
      }
      case "ProbeLeader": {
        return announceReady
      }
      case "RpcRequest": {
        if (frame.epoch !== options.epoch) return Effect.void
        return register(frame.from).pipe(
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The envelope schema leaves rpc payloads opaque; the rpc server validates them against its own request schemas and answers unknown tags with a defect.
          Effect.flatMap((clientId) => writeRequest(clientId, frame.message as FromClientEncoded))
        )
      }
      default: {
        return Effect.void
      }
    }
  }

  let subscribeOptions: SubscribeOptions = { onFrame }
  if (options.onVersionSkew !== undefined) {
    subscribeOptions = { onFrame, onVersionSkew: options.onVersionSkew }
  }
  yield* subscribeFrames(options.connection, subscribeOptions)

  const sweep = Effect.fnUntraced(function*() {
    const now = yield* Clock.currentTimeMillis
    const expired: Array<Wire.TabId> = []
    for (const [tabId, entry] of byTabId) {
      if (now - entry.lastSeen > clientTimeoutMillis) expired.push(tabId)
    }
    yield* Effect.forEach(expired, evict, { discard: true })
  })
  yield* Effect.sleep(sweepMillis).pipe(
    Effect.andThen(sweep()),
    Effect.forever,
    Effect.forkScoped
  )

  const protocol = yield* RpcServer.Protocol.make((write) =>
    Effect.sync(() => {
      writeRequest = write
      return {
        disconnects,
        send: (clientId, response) =>
          Effect.suspend(() => {
            const tabId = byClientId.get(clientId)
            if (tabId === undefined) return Effect.void
            return postFrame(
              options.connection,
              Wire.RpcResponse.make({ epoch: options.epoch, to: tabId, message: response })
            )
          }),
        end: () => Effect.void,
        clientIds: Effect.sync(() => clientIds),
        initialMessage: Effect.succeed(Option.none()),
        supportsAck: true,
        supportsTransferables: false,
        supportsSpanPropagation: false
      }
    })
  )

  yield* announceReady

  const result: ServerProtocol = {
    protocol,
    tabIdOf: (clientId) => byClientId.get(clientId),
    isConnected: (tabId) => byTabId.has(tabId),
    tabDisconnects
  }
  return result
})
