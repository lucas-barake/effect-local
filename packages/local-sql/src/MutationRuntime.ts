import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import type * as Identity from "@lucas-barake/effect-local/Identity"
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

export interface CurrentMutationView {
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly mutationId: Identity.MutationId
  readonly localSequence: Identity.LocalSequence
  readonly basis: Identity.ServerSequence
  readonly name: string
  readonly payload: typeof Schema.Json.Type
  readonly mutationVersion: Identity.SchemaVersion
}

export class MutationRuntime extends Context.Service<MutationRuntime, {
  readonly schemaIdentity: Identity.SchemaIdentity
  readonly migrationHash: Identity.SchemaHash
  readonly execute: (
    name: string,
    payload: unknown,
    transaction: Transaction.Transaction,
    changes: Array<Protocol.EntityChange>
  ) => Effect.Effect<Result, ReplicaError.ReplicaError>
  readonly prepare: (
    envelope: Protocol.MutationEnvelope
  ) => Effect.Effect<CurrentMutationView, ReplicaError.ReplicaError>
  readonly executeEnvelope: (
    envelope: Protocol.MutationEnvelope,
    transaction: Transaction.Transaction,
    changes: Array<Protocol.EntityChange>
  ) => Effect.Effect<Result, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/MutationRuntime") {}

export type Handlers<D extends Definition.Any,> = D["mutations"][number] extends infer M
  ? M extends Mutation.Mutation<infer Name, infer P, infer A, infer E> ? Mutation.HandlerService<Name, P, A, E> : never
  : never

type AnyHandler = Mutation.HandlerService<
  Mutation.Any["name"],
  Mutation.Any["payloadSchema"],
  Mutation.Any["successSchema"],
  Mutation.ErrorSchema
>

export const layer = <D extends Definition.Any,>(
  definition: D,
  evolution: Evolution.Evolution = Evolution.make({ current: definition })
): Layer.Layer<MutationRuntime, never, Handlers<D>> =>
  Layer.effect(
    MutationRuntime,
    Effect.gen(function*() {
      const context = yield* Effect.context<Handlers<D>>()
      const handlers = new Map<string, AnyHandler>()
      for (const mutation of definition.mutations) {
        handlers.set(mutation.name, Context.getUnsafe<AnyHandler, AnyHandler>(mutation.handler)(context))
      }
      const execute = Effect.fnUntraced(function*(
        name: string,
        payload: unknown,
        transaction: Transaction.Transaction,
        changes: Array<Protocol.EntityChange>
      ) {
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
          if (Schema.is(ReplicaError.ReplicaError)(failure)) return yield* Effect.fail(failure)
          return yield* Effect.die(failure)
        }
        const encoded = yield* Codec.encode(mutation.successSchema, result.success)
        const json = yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
          Effect.mapError((cause) =>
            new ReplicaError.StorageCorrupt({ message: "Mutation success is not JSON", cause })
          )
        )
        return Result_.succeed({ result: json, changes })
      })
      const prepare = (envelope: Protocol.MutationEnvelope) =>
        Evolution.migrateMutationPayload({
          evolution,
          source: envelope.sourceSchema,
          mutation: envelope.name,
          mutationVersion: envelope.mutationVersion,
          value: envelope.payload
        }).pipe(
          Effect.map((migrated) => ({
            spaceId: envelope.spaceId,
            clientId: envelope.clientId,
            mutationId: envelope.mutationId,
            localSequence: envelope.localSequence,
            basis: envelope.basis,
            name: envelope.name,
            payload: migrated.value,
            mutationVersion: migrated.mutationVersion
          }))
        )
      return MutationRuntime.of({
        schemaIdentity: evolution.current.schemaIdentity,
        migrationHash: evolution.migrationHash,
        execute,
        prepare,
        executeEnvelope: (envelope, transaction, changes) =>
          prepare(envelope).pipe(
            Effect.flatMap((current) => execute(current.name, current.payload, transaction, changes))
          )
      })
    })
  )
