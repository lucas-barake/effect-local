import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import { constVoid } from "effect/Function"
import * as Latch from "effect/Latch"
import * as Option from "effect/Option"
import * as Pull from "effect/Pull"
import * as Result from "effect/Result"
import { RpcClient, RpcClientError, RpcMessage, RpcSerialization } from "effect/rpc"
import * as Schedule from "effect/Schedule"
import * as Scheduler from "effect/Scheduler"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Socket from "effect/socket/Socket"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { reconnectPolicy } from "./configuration.js"

export interface Options {
  readonly retryTransientErrors?: boolean
  readonly retryPolicy?: Schedule.Schedule<any, Socket.SocketError>
}

const RequestId = Schema.Union([Schema.String, Schema.Number])
const Exit = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
  Schema.Struct({
    _tag: Schema.Literal("Failure"),
    cause: Schema.Union([
      Schema.Struct({ _tag: Schema.Literal("Fail"), error: Schema.Unknown }),
      Schema.Struct({ _tag: Schema.Literal("Die"), defect: Schema.Unknown }),
      Schema.Struct({ _tag: Schema.Literal("Interrupt"), fiberId: Schema.NullOr(Schema.Number) })
    ]).pipe(Schema.Array)
  })
])
const FromServer = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Chunk"),
    requestId: RequestId,
    values: Schema.NonEmptyArray(Schema.Unknown)
  }),
  Schema.Struct({ _tag: Schema.Literal("Exit"), requestId: RequestId, exit: Exit }),
  Schema.Struct({ _tag: Schema.Literal("Defect"), defect: Schema.Unknown }),
  Schema.Struct({ _tag: Schema.Literal("Pong") })
])
const FromServerMessages = Schema.Array(FromServer)
type WireFromServer = typeof FromServer["Type"]

function fromJsonWire(response: WireFromServer): RpcMessage.FromServerEncoded
function fromJsonWire(response: WireFromServer): WireFromServer | RpcMessage.FromServerEncoded {
  return response
}

export interface ProtocolSocket {
  readonly protocol: RpcClient.Protocol["Service"]
  readonly connections: SubscriptionRef.SubscriptionRef<number>
}

export const make = Effect.fnUntraced(function*(options?: Options): Effect.fn.Return<
  ProtocolSocket,
  never,
  Scope.Scope | RpcSerialization.RpcSerialization | Socket.Socket
> {
  const connections = yield* SubscriptionRef.make(0)
  const protocol = yield* makeProtocol(options, connections)
  return { protocol, connections }
})

const makeProtocol = (
  options: Options | undefined,
  connections: SubscriptionRef.SubscriptionRef<number>
): Effect.Effect<
  RpcClient.Protocol["Service"],
  never,
  Scope.Scope | RpcSerialization.RpcSerialization | Socket.Socket
> =>
  RpcClient.Protocol.make(Effect.fnUntraced(function*(writeResponse, clientIds) {
    const socket = yield* Socket.Socket
    const serialization = yield* RpcSerialization.RpcSerialization
    const hooks = yield* Effect.serviceOption(RpcClient.ConnectionHooks)
    const requestClientMap = new Map<string | number, number>()
    const writer = yield* socket.writer
    let parser = serialization.makeUnsafe()
    const pinger = yield* makePinger(Effect.suspend(() => writer.write(parser.encode(RpcMessage.constPing)!)))
    let currentError: RpcClientError.RpcClientError | undefined
    const broadcast = (response: RpcMessage.FromServerEncoded) =>
      Effect.forEach(clientIds, (clientId) => writeResponse(clientId, response))
    const failCurrentSocket = (error: RpcClientError.RpcClientError) => {
      currentError = error
      // Routes belong to one socket epoch. A late response from a closed epoch must not target its former client.
      requestClientMap.clear()
      return broadcast({ _tag: "ClientProtocolError", error })
    }
    const processFrame = Effect.fnUntraced(function*(message: Uint8Array | string) {
      const decoded = Effect.try({
        try: () => parser.decode(message),
        catch: (cause) =>
          new RpcClientError.RpcClientDefect({
            message: "Error decoding message",
            cause
          })
      }).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(FromServerMessages)),
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new RpcClientError.RpcClientDefect({
              message: "Error decoding message",
              cause
            })
          ))
      )
      const result = yield* Effect.result(decoded)
      if (Result.isFailure(result)) {
        yield* failCurrentSocket(new RpcClientError.RpcClientError({ reason: result.failure }))
        yield* new Socket.SocketError({ reason: new Socket.SocketReadError({ cause: result.failure }) })
        return
      }
      const responses = result.success
      let index = 0
      yield* Effect.whileLoop({
        while: () => index < responses.length,
        body: () => {
          // Effect's JSON codec represents an absent interrupt fiber id as null while its encoded type says undefined.
          const response = fromJsonWire(responses[index++])
          if (response._tag === "Pong") {
            pinger.onPong()
            return Effect.void
          }
          if (response._tag === "Chunk" || response._tag === "Exit") {
            const clientId = requestClientMap.get(response.requestId)
            if (clientId !== undefined) {
              if (response._tag === "Chunk") {
                return writeResponse(clientId, response).pipe(
                  Effect.catchDefect(() => {
                    const encoded = parser.encode({ _tag: "Interrupt", requestId: response.requestId })
                    if (encoded === undefined) return Effect.void
                    return writer.write(encoded)
                  })
                )
              }
              requestClientMap.delete(response.requestId)
              return writeResponse(clientId, response)
            }
          }
          return broadcast(response)
        },
        step: constVoid
      })
    })

    let connected = false
    const readFrames = Effect.gen(function*() {
      const { pull } = yield* socket.reader.pipe(Effect.provideService(Scheduler.PreventSchedulerYield, true))
      currentError = undefined
      if (Option.isSome(hooks)) yield* hooks.value.onConnect
      yield* writer.write(parser.encode(RpcMessage.constPing)!)
      while (true) {
        const frames = yield* pull
        if (!connected) {
          connected = true
          yield* SubscriptionRef.update(connections, (generation) => generation + 1)
        }
        for (const frame of frames) yield* processFrame(frame)
      }
    })

    const connection = Effect.suspend(() => {
      parser = serialization.makeUnsafe()
      pinger.reset()
      return readFrames.pipe(
        Effect.scoped,
        Effect.raceFirst(Effect.flatMap(
          pinger.timeout,
          () =>
            Effect.fail(
              new Socket.SocketError({
                reason: new Socket.SocketOpenError({
                  kind: "Timeout",
                  cause: "ping timeout"
                })
              })
            )
        ))
      )
    }).pipe(
      Effect.ensuring(Option.match(hooks, {
        onNone: () => Effect.void,
        onSome: (connectionHooks) => connectionHooks.onDisconnect
      })),
      Effect.tapCause((cause) => {
        const error = Cause.findError(cause)
        if (Result.isSuccess(error)) {
          if (
            options?.retryTransientErrors &&
            error.success.reason._tag === "SocketOpenError" &&
            requestClientMap.size === 0
          ) return Effect.void
          return failCurrentSocket(new RpcClientError.RpcClientError({ reason: error.success.reason }))
        }
        const reason = new RpcClientError.RpcClientDefect({
          message: "Unknown socket error",
          cause
        })
        return failCurrentSocket(new RpcClientError.RpcClientError({ reason }))
      })
    )
    const retryPolicy = options?.retryPolicy ?? reconnectPolicy

    yield* Effect.gen(function*() {
      let step = yield* Schedule.toStepWithMetadata(retryPolicy)
      while (true) {
        connected = false
        const error = yield* Effect.flip(connection)
        if (connected) step = yield* Schedule.toStepWithMetadata(retryPolicy)
        const retrying = yield* step(error).pipe(
          Effect.as(true),
          Pull.catchDone(() =>
            failCurrentSocket(new RpcClientError.RpcClientError({ reason: error.reason })).pipe(Effect.as(false))
          )
        )
        if (!retrying) return
      }
    }).pipe(
      Effect.annotateLogs({
        module: "RpcClient",
        method: "makeProtocolSocket"
      }),
      Effect.forkScoped
    )

    return {
      send(clientId: number, request: RpcMessage.FromClientEncoded) {
        if (request._tag === "Interrupt") requestClientMap.delete(request.requestId)
        if (currentError) return Effect.fail(currentError)
        if (request._tag === "Request") requestClientMap.set(request.id, clientId)
        const encoded = parser.encode(request)
        if (encoded === undefined) return Effect.void
        return writer.write(encoded).pipe(
          Effect.catchTag("SocketError", (error) => {
            if (request._tag === "Request") requestClientMap.delete(request.id)
            return Effect.fail(new RpcClientError.RpcClientError({ reason: error.reason }))
          })
        )
      },
      supportsAck: true,
      supportsTransferables: false,
      codecFor: serialization.codecFor
    }
  }))

const makePinger = Effect.fnUntraced(function*<A, E extends { readonly _tag: string }, R,>(
  writePing: Effect.Effect<A, E, R>
) {
  let receivedPong = true
  const latch = Latch.makeUnsafe()
  const reset = () => {
    receivedPong = true
    latch.closeUnsafe()
  }
  const onPong = () => {
    receivedPong = true
  }
  yield* Effect.suspend((): Effect.Effect<void, E, R> => {
    if (!receivedPong) return latch.open
    receivedPong = false
    return writePing
  }).pipe(
    Effect.delay("5 seconds"),
    Effect.ignore,
    Effect.forever,
    Effect.interruptible,
    Effect.forkScoped
  )
  return { timeout: latch.await, reset, onPong } as const
})
