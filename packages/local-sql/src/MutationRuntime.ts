import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Transaction from "@lucas-barake/effect-local/Transaction"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result_ from "effect/Result"
import * as Schema from "effect/Schema"
import * as Codec from "./internal/codec.js"

export type Result = Result_.Result<{
  readonly result: typeof Schema.Json.Type
  readonly changes: ReadonlyArray<Protocol.EntityChange>
}, typeof Schema.Json.Type>

export class MutationRuntime extends Context.Service<MutationRuntime, {
  readonly execute: (
    name: string,
    payload: unknown,
    transaction: Transaction.Transaction,
    changes: Array<Protocol.EntityChange>
  ) => Effect.Effect<Result, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/MutationRuntime") {}

export type Handlers<D extends Definition.Any,> = D["mutations"][number] extends infer M
  ? M extends Mutation.Mutation<infer Name, infer P, infer A, infer E> ? Mutation.HandlerService<Name, P, A, E> : never
  : never

export const layer = <D extends Definition.Any,>(definition: D): Layer.Layer<MutationRuntime, never, Handlers<D>> =>
  Layer.effect(
    MutationRuntime,
    Effect.gen(function*() {
      const context = yield* Effect.context<Handlers<D>>()
      const handlers = new Map<string, Mutation.HandlerService<any, any, any, any>>()
      for (const mutation of definition.mutations) {
        handlers.set(mutation.name, Context.get(context, mutation.handler as any) as any)
      }
      return MutationRuntime.of({
        execute: (name, payload, transaction, changes) =>
          Effect.gen(function*() {
            const mutation = definition.mutationByName.get(name)
            const handler = handlers.get(name)
            if (mutation === undefined || handler === undefined) {
              return yield* new ReplicaError.ProtocolInvalid({ message: `Unknown mutation: ${name}` })
            }
            const decodedPayload = yield* Codec.decode(mutation.payloadSchema, payload)
            const result = yield* handler.execute({ transaction, payload: decodedPayload }).pipe(Effect.result)
            if (Result_.isFailure(result)) {
              const failure = result.failure
              if (Schema.is(mutation.rejectionSchema)(failure)) {
                const encoded = yield* Codec.encode(mutation.rejectionSchema, failure)
                const json = yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
                  Effect.mapError((cause) =>
                    new ReplicaError.StorageCorrupt({ message: "Mutation rejection is not JSON", cause })
                  )
                )
                return Result_.fail(json)
              }
              return yield* Effect.fail(failure as ReplicaError.ReplicaError)
            }
            const encoded = yield* Codec.encode(mutation.successSchema, result.success)
            const json = yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
              Effect.mapError((cause) =>
                new ReplicaError.StorageCorrupt({ message: "Mutation success is not JSON", cause })
              )
            )
            return Result_.succeed({ result: json, changes })
          })
      })
    })
  )
