import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import type * as RunnerAddress from "effect/unstable/cluster/RunnerAddress"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import { RpcClientDefect, RpcClientError } from "effect/unstable/rpc/RpcClientError"
import type * as RpcMessage from "effect/unstable/rpc/RpcMessage"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as lockNames from "./lockNames.js"
import * as LosslessQueue from "./losslessQueue.js"
import type * as platform from "./platform.js"

const RequestId = Schema.Union([Schema.String, Schema.Number])

const Header = Schema.mutable(Schema.Tuple([Schema.String, Schema.String]))

const WireRequest = Schema.Struct({
  _tag: Schema.Literal("Request"),
  id: RequestId,
  tag: Schema.String,
  payload: Schema.Unknown,
  headers: Schema.Array(Header),
  isNotification: Schema.optionalKey(Schema.Literal(true)),
  traceId: Schema.optionalKey(Schema.String),
  spanId: Schema.optionalKey(Schema.String),
  sampled: Schema.optionalKey(Schema.Boolean)
})

const FromClient = Schema.Union([
  WireRequest,
  Schema.Struct({ _tag: Schema.Literal("Ack"), requestId: RequestId }),
  Schema.Struct({ _tag: Schema.Literal("Interrupt"), requestId: RequestId }),
  Schema.Struct({ _tag: Schema.Literal("Ping") }),
  Schema.Struct({ _tag: Schema.Literal("Eof") })
])

const WireExit = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
  Schema.Struct({
    _tag: Schema.Literal("Failure"),
    cause: Schema.Array(Schema.Union([
      Schema.Struct({ _tag: Schema.Literal("Fail"), error: Schema.Unknown }),
      Schema.Struct({ _tag: Schema.Literal("Die"), defect: Schema.Unknown }),
      Schema.Struct({ _tag: Schema.Literal("Interrupt"), fiberId: Schema.UndefinedOr(Schema.Number) })
    ]))
  })
])

const FromServer = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Chunk"), requestId: RequestId, values: Schema.NonEmptyArray(Schema.Unknown) }),
  Schema.Struct({ _tag: Schema.Literal("Exit"), requestId: RequestId, exit: WireExit }),
  Schema.Struct({ _tag: Schema.Literal("Defect"), defect: Schema.Unknown }),
  Schema.Struct({ _tag: Schema.Literal("Pong") })
])

const Frame = Schema.Union([
  Schema.TaggedStruct("ToServer", { from: Schema.String, connection: Schema.Int, message: FromClient }),
  Schema.TaggedStruct("ToClient", { from: Schema.String, connection: Schema.Int, message: FromServer })
])
type Frame = typeof Frame.Type

const decodeFrame = Schema.decodeUnknownEffect(Frame)
const encodeFrame = Schema.encodeEffect(Frame)

export interface TabTransport {
  readonly server: RpcServer.Protocol["Service"]
  readonly clients: Runners.RpcClientProtocol["Service"]
}

export interface Options {
  readonly name: string
  readonly self: RunnerAddress.RunnerAddress
  readonly locks: platform.WebLocksService
  readonly channels: platform.TabChannelService
}

type ServerMessage = typeof FromServer.Type

export const make = Effect.fnUntraced(function*(options: Options) {
  const transportScope = yield* Effect.scope
  const self = options.self.host
  const names = lockNames.make(options.name)
  const outboxes = new Map<string, Deferred.Deferred<platform.TabChannelConnection>>()

  const openOutbox = Effect.fnUntraced(function*(
    host: string,
    opened: Deferred.Deferred<platform.TabChannelConnection>
  ) {
    const scope = yield* Scope.fork(transportScope)
    const connection = yield* options.channels.open(names.inbox(host)).pipe(Scope.provide(scope))
    yield* Deferred.succeed(opened, connection)
    yield* options.locks.released(names.runner(host)).pipe(
      Effect.andThen(Effect.sync(() => outboxes.delete(host))),
      Effect.andThen(Scope.close(scope, Exit.void)),
      Effect.forkIn(transportScope)
    )
  })

  const outboxFor = (host: string) =>
    Effect.suspend(() => {
      const known = outboxes.get(host)
      if (known !== undefined) return Deferred.await(known)
      const opened = Deferred.makeUnsafe<platform.TabChannelConnection>()
      outboxes.set(host, opened)
      return openOutbox(host, opened).pipe(Effect.uninterruptible, Effect.andThen(Deferred.await(opened)))
    })

  const post = (host: string, frame: Frame) =>
    encodeFrame(frame).pipe(
      Effect.catchTag("SchemaError", (error) => Effect.die(error)),
      Effect.flatMap((encoded) => outboxFor(host).pipe(Effect.flatMap((outbox) => outbox.post(encoded))))
    )

  interface ServerPeer {
    readonly host: string
    readonly connection: number
  }

  const serverClients = new Map<string, Map<number, number>>()
  const serverPeers = new Map<number, ServerPeer>()
  const serverClientIds = new Set<number>()
  const disconnects = yield* Queue.unbounded<number>()
  let nextClientId = 0
  let writeRequest: (clientId: number, data: RpcMessage.FromClientEncoded) => Effect.Effect<void> = () => Effect.void

  const forgetServerClient = (clientId: number) => {
    const peer = serverPeers.get(clientId)
    if (peer === undefined) return
    serverPeers.delete(clientId)
    serverClientIds.delete(clientId)
    const connections = serverClients.get(peer.host)
    if (connections === undefined) return
    connections.delete(peer.connection)
    if (connections.size === 0) serverClients.delete(peer.host)
  }

  const forgetServerHost = (host: string) =>
    Effect.suspend(() => {
      const connections = serverClients.get(host)
      if (connections === undefined) return Effect.void
      const clientIds = Array.from(connections.values())
      for (const clientId of clientIds) forgetServerClient(clientId)
      return Queue.offerAll(disconnects, clientIds)
    })

  const serverClientFor = (host: string, connection: number): Effect.Effect<number> =>
    Effect.suspend(() => {
      const known = serverClients.get(host)
      const existing = known?.get(connection)
      if (existing !== undefined) return Effect.succeed(existing)
      const clientId = nextClientId++
      serverPeers.set(clientId, { host, connection })
      serverClientIds.add(clientId)
      if (known !== undefined) {
        known.set(connection, clientId)
        return Effect.succeed(clientId)
      }
      serverClients.set(host, new Map([[connection, clientId]]))
      return options.locks.released(names.runner(host)).pipe(
        Effect.andThen(forgetServerHost(host)),
        Effect.forkIn(transportScope),
        Effect.as(clientId)
      )
    })

  const server = yield* RpcServer.Protocol.make((write) =>
    Effect.sync(() => {
      writeRequest = write
      return {
        disconnects,
        send: (clientId: number, response: RpcMessage.FromServerEncoded) =>
          Effect.suspend(() => {
            const peer = serverPeers.get(clientId)
            if (peer === undefined) return Effect.void
            if (response._tag === "ClientProtocolError" || response._tag === "Request") {
              return Effect.die(`The tab transport cannot carry a server ${response._tag} message`)
            }
            return post(peer.host, { _tag: "ToClient", from: self, connection: peer.connection, message: response })
          }),
        end: (clientId: number) =>
          Effect.sync(() => {
            forgetServerClient(clientId)
          }),
        clientIds: Effect.sync(() => serverClientIds),
        initialMessage: Effect.succeedNone,
        supportsAck: false,
        supportsTransferables: false,
        supportsSpanPropagation: true,
        supportsNotifications: false,
        codecFor: RpcSerialization.json.codecFor
      }
    })
  )

  const targets = new Map<number, (message: ServerMessage) => Effect.Effect<void>>()
  let nextConnection = 0

  const dispatch = (frame: Frame): Effect.Effect<void> => {
    if (frame._tag === "ToServer") {
      return serverClientFor(frame.from, frame.connection).pipe(
        Effect.flatMap((clientId) => writeRequest(clientId, fromClientWire(frame.message)))
      )
    }
    const target = targets.get(frame.connection)
    if (target === undefined) return Effect.void
    return target(frame.message)
  }

  const inbox = yield* options.channels.open(names.inbox(self)).pipe(Effect.flatMap((channel) => channel.messages))
  yield* LosslessQueue.take(inbox).pipe(
    Effect.flatMap((raw) =>
      decodeFrame(raw).pipe(
        Effect.flatMap(dispatch),
        Effect.catchTag("SchemaError", (error) =>
          Effect.logWarning("dropped a malformed tab transport frame").pipe(
            Effect.annotateLogs({ error: error.message })
          ))
      )
    ),
    Effect.forever,
    Effect.forkIn(transportScope)
  )

  const clientFor = (address: RunnerAddress.RunnerAddress) =>
    RpcClient.Protocol.make(Effect.fnUntraced(function*(writeResponse, clientIds) {
      const scope = yield* Effect.scope
      const target = address.host
      const connection = nextConnection++
      const requestClients = new Map<string | number, number>()
      let failure: RpcClientError | undefined
      const broadcast = (response: RpcMessage.FromServerEncoded) =>
        Effect.forEach(clientIds, (clientId) => writeResponse(clientId, response), { discard: true })
      targets.set(connection, (message) => {
        const response = fromServerWire(message)
        if (response._tag === "Chunk" || response._tag === "Exit") {
          const clientId = requestClients.get(response.requestId)
          if (clientId === undefined) return Effect.void
          if (response._tag === "Exit") requestClients.delete(response.requestId)
          return writeResponse(clientId, response)
        }
        return broadcast(response)
      })
      yield* Scope.addFinalizer(
        scope,
        Effect.sync(() => targets.delete(connection)).pipe(
          Effect.andThen(Effect.suspend(() => {
            if (failure !== undefined) return Effect.void
            return post(target, { _tag: "ToServer", from: self, connection, message: { _tag: "Eof" } })
          }))
        )
      )
      yield* options.locks.released(names.runner(target)).pipe(
        Effect.andThen(Effect.suspend(() => {
          failure = new RpcClientError({
            reason: new RpcClientDefect({ message: `Runner ${target} is no longer running`, cause: undefined })
          })
          requestClients.clear()
          return broadcast({ _tag: "ClientProtocolError", error: failure })
        })),
        Effect.forkIn(scope)
      )
      return {
        send: (clientId: number, request: RpcMessage.FromClientEncoded) =>
          Effect.suspend(() => {
            if (failure !== undefined) return Effect.fail(failure)
            if (request._tag === "Request") requestClients.set(request.id, clientId)
            if (request._tag === "Interrupt") requestClients.delete(request.requestId)
            return post(target, { _tag: "ToServer", from: self, connection, message: request })
          }),
        supportsAck: false,
        supportsTransferables: false,
        codecFor: RpcSerialization.json.codecFor
      }
    }))

  const transport: TabTransport = {
    server,
    clients: Runners.RpcClientProtocol.of({ make: clientFor, codecFor: RpcSerialization.json.codecFor })
  }
  return transport
})

function fromClientWire(message: typeof FromClient.Type): RpcMessage.FromClientEncoded
function fromClientWire(message: typeof FromClient.Type): typeof FromClient.Type | RpcMessage.FromClientEncoded {
  return message
}

function fromServerWire(message: ServerMessage): RpcMessage.FromServerEncoded
function fromServerWire(message: ServerMessage): ServerMessage | RpcMessage.FromServerEncoded {
  return message
}
