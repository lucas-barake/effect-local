import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import type * as Document from "@lucas-barake/effect-local/Document"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { RpcServer } from "effect/unstable/rpc"
import * as Wire from "./internal/wire.js"
import * as ReplicaRpc from "./ReplicaRpc.js"
import * as SessionManager from "./SessionManager.js"

const lookup = <A,>(
  values: ReadonlyMap<string, A>,
  kind: string,
  name: string
): Effect.Effect<A, ReplicaError.ReplicaError> => {
  const value = values.get(name)
  return value === undefined
    ? Effect.fail(
      new ReplicaError.ReplicaError({
        reason: { _tag: "ProtocolMismatch", expected: `known ${kind}`, observed: name }
      })
    )
    : Effect.succeed(value)
}

export const handlers = (definition: ReplicaDefinition.Any) =>
  ReplicaRpc.group.toLayer(Effect.gen(function*() {
    const replica = yield* Replica.Replica
    const sessions = yield* SessionManager.SessionManager
    const commits = yield* CommitPublisher.CommitPublisher
    const ownerEpoch = globalThis.crypto.randomUUID()
    const documents = new Map<string, Document.Any>(
      definition.documents.documents.map((document: Document.Any) => [document.name, document])
    )
    const mutations = new Map<string, Mutation.Any>(
      definition.mutations.map((mutation: Mutation.Any) => [mutation.name, mutation])
    )
    const queries = new Map<string, Query.Any>(
      definition.queries.map((query: Query.Any) => [query.name, query])
    )
    const allInvalidationKeys = ReplicaDefinition.invalidationKeys(definition)
    return ReplicaRpc.group.of({
      OpenSession: ({ definitionHash, sessionId }, { client }) =>
        definitionHash === definition.hash
          ? sessions.open(sessionId, client.id).pipe(Effect.as({
            leaseMillis: SessionManager.leaseDurationMillis,
            protocolVersion: ReplicaRpc.protocolVersion,
            definitionHash: definition.hash,
            ownerEpoch
          }))
          : Effect.fail(
            new ReplicaError.ReplicaError({
              reason: { _tag: "ProtocolMismatch", expected: definition.hash, observed: definitionHash }
            })
          ),
      RenewSession: ({ sessionId }, { client }) =>
        sessions.renew(sessionId, client.id).pipe(Effect.as({ leaseMillis: SessionManager.leaseDurationMillis })),
      CloseSession: ({ sessionId }, { client }) => sessions.close(sessionId, client.id),
      Create: ({ commandId, document, sessionId, value }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              Wire.decode(definition.schema, value).pipe(
                Effect.flatMap((decoded) => replica.create(definition, { commandId, value: decoded } as never))
              )
            )
          )
        ),
      Get: ({ document, documentId, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              replica.get(definition, documentId).pipe(
                Effect.flatMap((snapshot) =>
                  Wire.encode(definition.schema, snapshot.value).pipe(
                    Effect.map((value) => ({ ...snapshot, value }))
                  )
                )
              )
            )
          )
        ),
      Mutate: ({ commandId, documentId, mutation, payload, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(mutations, "mutation", mutation).pipe(
            Effect.flatMap((definition) =>
              Wire.decode(definition.payload, payload).pipe(
                Effect.flatMap((decoded) =>
                  replica.mutate(definition, { commandId, documentId, payload: decoded } as never)
                ),
                Effect.flatMap((outcome) => Wire.encodeOutcome(definition.success, definition.error, outcome))
              )
            )
          )
        ),
      Delete: ({ commandId, document, documentId, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              replica.delete(definition, { commandId, documentId }).pipe(
                Effect.flatMap((outcome) => Wire.encodeOutcome(Schema.Void, Schema.Never, outcome))
              )
            )
          )
        ),
      Query: ({ payload, query, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(queries, "query", query).pipe(
            Effect.flatMap((definition) =>
              Wire.decode(definition.payload, payload).pipe(
                Effect.flatMap((decoded) =>
                  (replica.query as unknown as (
                    query: Query.Any,
                    payload: unknown
                  ) => Effect.Effect<unknown, unknown>)(definition, decoded)
                ),
                Effect.flatMap((result) => Wire.encode(definition.success, result)),
                Effect.catch((error) =>
                  Schema.is(ReplicaError.ReplicaError)(error)
                    ? Effect.fail(error)
                    : Wire.encode(definition.error, error).pipe(
                      Effect.flatMap((encoded) => Effect.fail({ _tag: "ReplicaQueryError" as const, error: encoded }))
                    )
                )
              )
            )
          )
        ),
      LookupMutation: ({ commandId, mutation, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(mutations, "mutation", mutation).pipe(
            Effect.flatMap((definition) =>
              replica.lookupMutation(definition, commandId).pipe(
                Effect.flatMap((outcome) => Wire.encodeOutcome(definition.success, definition.error, outcome))
              )
            )
          )
        ),
      LookupCreate: ({ commandId, document, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) => replica.lookupCreate(definition, commandId))
          )
        ),
      LookupDelete: ({ commandId, document, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              replica.lookupDelete(definition, commandId).pipe(
                Effect.flatMap((outcome) => Wire.encodeOutcome(Schema.Void, Schema.Never, outcome))
              )
            )
          )
        ),
      Flush: ({ sessionId }, { client }) => sessions.run(sessionId, client.id, replica.flush),
      Invalidations: ({ ownerEpoch: requestedEpoch, sessionId }, { client }) =>
        requestedEpoch === ownerEpoch
          ? sessions.stream(
            sessionId,
            client.id,
            Stream.scoped(Stream.unwrap(commits.subscribe.pipe(
              Effect.map((subscription) =>
                Stream.make({
                  _tag: "InvalidationsReady",
                  ownerEpoch,
                  watermark: subscription.watermark,
                  refreshGeneration: subscription.refreshGeneration
                }).pipe(
                  Stream.concat(subscription.events.pipe(Stream.map((event): ReplicaRpc.InvalidationMessage =>
                    event._tag === "Commit"
                      ? {
                        _tag: "Invalidation",
                        ownerEpoch,
                        sequence: event.commitSequence,
                        keys: event.keys
                      }
                      : { _tag: "FullRefreshRequired", ownerEpoch, keys: allInvalidationKeys }
                  )))
                )
              )
            )))
          )
          : Stream.fail(
            new ReplicaError.ReplicaError({
              reason: { _tag: "ProtocolMismatch", expected: ownerEpoch, observed: requestedEpoch }
            })
          ),
      Status: ({ sessionId }, { client }) => sessions.stream(sessionId, client.id, replica.status),
      ExportBackup: ({ maxBytes, sessionId }, { client }) =>
        sessions.stream(
          sessionId,
          client.id,
          replica.exportBackup({ maxBytes }).pipe(
            Stream.map((chunk) => new Uint8Array(chunk))
          )
        ),
      RestoreBackup: ({ chunks, expectedDefinitionHash, maxBytes, mode, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          replica.restoreBackup({ source: Stream.fromIterable(chunks), expectedDefinitionHash, maxBytes, mode })
        ),
      ExportDocument: ({ document, documentId, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              replica.exportDocument(definition, documentId).pipe(
                Effect.flatMap((exported) =>
                  Wire.encode(Schema.toEncoded(definition.schema), exported.value).pipe(
                    Effect.map((value) => ({ ...exported, value }))
                  )
                )
              )
            )
          )
        ),
      ImportDocument: ({ commandId, document, sessionId, value }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              Wire.decode(Schema.toEncoded(definition.schema), value.value).pipe(
                Effect.flatMap((decoded) =>
                  replica.importDocument(definition, {
                    commandId,
                    value: { ...value, value: decoded }
                  } as never)
                )
              )
            )
          )
        )
    })
  }))

export const layer = (definition: ReplicaDefinition.Any) =>
  RpcServer.layer(ReplicaRpc.group).pipe(Layer.provide(handlers(definition)))

export const layerWorker = (definition: ReplicaDefinition.Any) =>
  layer(definition).pipe(Layer.provide(RpcServer.layerProtocolWorkerRunner))
