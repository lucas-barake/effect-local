import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import { constVoid } from "effect/Function"
import * as Latch from "effect/Latch"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import { RpcClient, RpcClientError, RpcMessage, RpcSerialization } from "effect/unstable/rpc"
import * as Socket from "effect/unstable/socket/Socket"

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

export const make = (options?: Options): Effect.Effect<
  RpcClient.Protocol["Service"],
  never,
  Scope.Scope | RpcSerialization.RpcSerialization | Socket.Socket
> => {
  const protocol = Effect.fnUntraced(function*(
    writeResponse: (clientId: number, response: RpcMessage.FromServerEncoded) => Effect.Effect<void>,
    clientIds: ReadonlySet<number>
  ) {
    const socket = yield* Socket.Socket
    const serialization = yield* RpcSerialization.RpcSerialization
    const hooks = yield* Effect.serviceOption(RpcClient.ConnectionHooks)
    const requestClientMap = new Map<string | number, number>()
    const write = yield* socket.writer
    let parser = serialization.makeUnsafe()
    const encodedPing = parser.encode(RpcMessage.constPing)!
    const writePing = write(encodedPing)
    const pinger = yield* makePinger(writePing)
    let currentError: RpcClientError.RpcClientError | undefined
    const onOpen = Effect.suspend(() => {
      currentError = undefined
      if (Option.isSome(hooks)) return hooks.value.onConnect
      return Effect.void
    })
    const broadcast = (response: RpcMessage.FromServerEncoded) =>
      Effect.forEach(clientIds, (clientId) => writeResponse(clientId, response))
    const failCurrentSocket = (error: RpcClientError.RpcClientError) => {
      currentError = error
      // Routes belong to one socket epoch. A late response from a closed epoch must not target its former client.
      requestClientMap.clear()
      return broadcast({ _tag: "ClientProtocolError", error })
    }

    yield* Effect.suspend(() => {
      parser = serialization.makeUnsafe()
      pinger.reset()
      return socket.runRaw((message) => {
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
        return Effect.gen(function*() {
          const result = yield* Effect.result(decoded)
          if (Result.isFailure(result)) {
            yield* failCurrentSocket(new RpcClientError.RpcClientError({ reason: result.failure }))
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
                  if (response._tag === "Exit") requestClientMap.delete(response.requestId)
                  return writeResponse(clientId, response)
                }
              }
              return broadcast(response)
            },
            step: constVoid
          })
        })
      }, { onOpen }).pipe(
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
      Effect.flatMap(() => {
        const reason = new Socket.SocketCloseError({ code: 1000 })
        return Effect.fail(new Socket.SocketError({ reason }))
      }),
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
      }),
      Effect.retry(options?.retryPolicy ?? defaultRetryPolicy),
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
        return write(encoded).pipe(Effect.catch((error) => Effect.die(error)))
      },
      supportsAck: true,
      supportsTransferables: false
    }
  })
  return RpcClient.Protocol.make(protocol)
}

const exponentialRetryPolicy = Schedule.exponential(500, 1.5)
const spacedRetryPolicy = Schedule.spaced(5000)
const defaultRetryPolicy = Schedule.min([exponentialRetryPolicy, spacedRetryPolicy])

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
