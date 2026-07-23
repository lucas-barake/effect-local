import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as ClusterSchema from "effect/unstable/cluster/ClusterSchema"
import * as Entity from "effect/unstable/cluster/Entity"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as Rpc from "effect/unstable/rpc/Rpc"
import * as CommandExecutor from "./CommandExecutor.js"
import * as PeerSync from "./PeerSync.js"
import * as ReplicaGate from "./ReplicaGate.js"

const commandFields = {
  replicaIncarnation: Identity.ReplicaIncarnation,
  writerGeneration: Identity.WriterGeneration,
  commandId: Identity.CommandId,
  documentType: Schema.String,
  requestHash: Schema.String
}

const primaryKey = (payload: {
  readonly replicaIncarnation: Identity.ReplicaIncarnation
  readonly commandId: Identity.CommandId
  readonly requestHash: string
}) => `${payload.replicaIncarnation}:${payload.commandId}:${payload.requestHash}`

const syncPrimaryKey = (payload: {
  readonly replicaIncarnation: Identity.ReplicaIncarnation
  readonly peerId: Identity.PeerId
  readonly connectionEpoch: string
  readonly receiveSequence: number
  readonly messageHash: string
}) =>
  JSON.stringify([
    payload.replicaIncarnation,
    payload.peerId,
    payload.connectionEpoch,
    payload.receiveSequence,
    payload.messageHash
  ])

export const Create = Rpc.make("Create", {
  payload: { ...commandFields, payload: Schema.Uint8ArrayFromBase64 },
  success: Schema.Uint8ArrayFromBase64,
  error: ReplicaError.ReplicaError,
  primaryKey
}).annotate(ClusterSchema.Persisted, true).annotate(ClusterSchema.WithTransaction, true).annotate(
  ClusterSchema.Uninterruptible,
  "client"
)

export const Mutate = Rpc.make("Mutate", {
  payload: { ...commandFields, mutationTag: Schema.String, payload: Schema.Uint8ArrayFromBase64 },
  success: Schema.Uint8ArrayFromBase64,
  error: ReplicaError.ReplicaError,
  primaryKey
}).annotate(ClusterSchema.Persisted, true).annotate(ClusterSchema.WithTransaction, true).annotate(
  ClusterSchema.Uninterruptible,
  "client"
)

export const Delete = Rpc.make("Delete", {
  payload: commandFields,
  success: Schema.Uint8ArrayFromBase64,
  error: ReplicaError.ReplicaError,
  primaryKey
}).annotate(ClusterSchema.Persisted, true).annotate(ClusterSchema.WithTransaction, true).annotate(
  ClusterSchema.Uninterruptible,
  "client"
)

export const ApplySyncResult = Schema.Struct({
  reply: Schema.NullOr(Schema.Struct({
    documentId: Identity.DocumentId,
    message: Schema.Uint8ArrayFromBase64,
    messageHash: Schema.String,
    heads: Schema.Array(Schema.String)
  })),
  heads: Schema.Array(Schema.String),
  acceptedHeads: Schema.Array(Schema.String),
  commitSequence: Identity.CommitSequence,
  observedByPeer: Schema.Boolean,
  durableConfirmation: Schema.Literal(false),
  duplicate: Schema.Boolean
})

export const ApplySync = Rpc.make("ApplySync", {
  payload: {
    replicaIncarnation: Identity.ReplicaIncarnation,
    peerId: Identity.PeerId,
    connectionEpoch: Schema.NonEmptyString,
    localConnectionEpoch: Schema.NonEmptyString,
    receiveSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    documentType: Schema.String,
    messageHash: Schema.String,
    message: Schema.Uint8ArrayFromBase64
  },
  success: ApplySyncResult,
  error: ReplicaError.ReplicaError,
  primaryKey: syncPrimaryKey
}).annotate(ClusterSchema.Persisted, true).annotate(ClusterSchema.WithTransaction, true).annotate(
  ClusterSchema.Uninterruptible,
  true
)

export const DocumentEntity = Entity.make("EffectLocal/Document", [Create, Mutate, Delete, ApplySync])

const decode = <S extends Document.WireSchema,>(
  schema: S,
  documentId: Identity.DocumentId,
  bytes: Uint8Array
) =>
  Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(new TextDecoder().decode(bytes)).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "schema-coded JSON",
          observed: String(cause)
        })
      })
    ),
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.toCodecJson(schema))),
    Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.DocumentDecodeError({
            documentId,
            cause
          })
        })
      ))
  )

const encode = <S extends Document.WireSchema,>(schema: S, value: S["Type"]) =>
  Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(schema)))(value).pipe(
    Effect.map((encoded) => new TextEncoder().encode(encoded)),
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.StorageCorrupt({
          cause
        })
      })
    )
  )

const resolveDocument = (definition: ReplicaDefinition.Any, name: string) => {
  const document = DocumentSet.get(definition.documents, name)
  return document === undefined
    ? Effect.fail(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "registered document type",
          observed: name
        })
      })
    )
    : Effect.succeed(document)
}

const resolveMutation = (
  definition: ReplicaDefinition.Any,
  document: Document.Any,
  name: string
): Effect.Effect<Mutation.Any, ReplicaError.ReplicaError> => {
  const mutation = definition.mutations.find((candidate: Mutation.Any) =>
    candidate.name === name && candidate.document === document
  )
  return mutation === undefined
    ? Effect.fail(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: `registered mutation for ${document.name}`,
          observed: name
        })
      })
    )
    : Effect.succeed(mutation)
}

export const layer = (definition: ReplicaDefinition.Any): Layer.Layer<
  never,
  never,
  | CommandExecutor.CommandExecutor
  | PeerSync.PeerSync
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
  | Crypto.Crypto
  | Sharding.Sharding
> =>
  Layer.unwrap(Effect.gen(function*() {
    const limits = yield* ReplicaLimits.ReplicaLimits
    const crypto = yield* Crypto.Crypto
    return DocumentEntity.toLayer(
      Effect.gen(function*() {
        const executor = yield* CommandExecutor.CommandExecutor
        const peerSync = yield* PeerSync.PeerSync
        const gate = yield* ReplicaGate.ReplicaGate
        const permit = (payload: {
          readonly replicaIncarnation: Identity.ReplicaIncarnation
          readonly writerGeneration: Identity.WriterGeneration
        }) =>
          gate.current.pipe(Effect.map((current) => ({
            replicaId: current.replicaId,
            incarnation: payload.replicaIncarnation,
            writerGeneration: payload.writerGeneration
          })))
        return DocumentEntity.of({
          Create: (request) => {
            const documentId = Identity.DocumentId.make(request.address.entityId.toString())
            return Effect.gen(function*() {
              const document = yield* resolveDocument(definition, request.payload.documentType)
              const value = yield* decode(document.schema, documentId, request.payload.payload)
              const outcome = yield* executor.create(document, {
                commandId: request.payload.commandId,
                documentId,
                permit: yield* permit(request.payload),
                requestHash: request.payload.requestHash,
                value
              })
              return yield* encode(CommandOutcome.schema(Identity.DocumentId, Schema.Never), outcome)
            })
          },
          Mutate: (request) => {
            const documentId = Identity.DocumentId.make(request.address.entityId.toString())
            return Effect.gen(function*() {
              const document = yield* resolveDocument(definition, request.payload.documentType)
              const mutation = yield* resolveMutation(definition, document, request.payload.mutationTag)
              const payload = yield* decode(mutation.payloadSchema, documentId, request.payload.payload)
              const outcome = yield* executor.mutate(mutation, {
                commandId: request.payload.commandId,
                documentId,
                payload,
                permit: yield* permit(request.payload),
                requestHash: request.payload.requestHash
              })
              return yield* encode(
                CommandOutcome.schema(mutation.successSchema, mutation.errorSchema),
                outcome
              )
            })
          },
          Delete: (request) => {
            const documentId = Identity.DocumentId.make(request.address.entityId.toString())
            return Effect.gen(function*() {
              const document = yield* resolveDocument(definition, request.payload.documentType)
              const outcome = yield* executor.delete(document, {
                commandId: request.payload.commandId,
                documentId,
                permit: yield* permit(request.payload),
                requestHash: request.payload.requestHash
              })
              return yield* encode(CommandOutcome.schema(Schema.Void, Schema.Never), outcome)
            })
          },
          ApplySync: (request) => {
            const documentId = Identity.DocumentId.make(request.address.entityId.toString())
            return Effect.gen(function*() {
              const document = yield* resolveDocument(definition, request.payload.documentType)
              const current = yield* gate.current
              if (request.payload.replicaIncarnation !== current.incarnation) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: String(current.incarnation),
                    observed: String(request.payload.replicaIncarnation)
                  })
                })
              }
              const messageHash = yield* Canonical.digest(request.payload.message).pipe(
                Effect.provideService(Crypto.Crypto, crypto)
              )
              if (messageHash !== request.payload.messageHash) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: messageHash,
                    observed: request.payload.messageHash
                  })
                })
              }
              return yield* peerSync.receive(
                document,
                documentId,
                {
                  peerId: request.payload.peerId,
                  connectionEpoch: request.payload.localConnectionEpoch,
                  replicaIncarnation: request.payload.replicaIncarnation
                },
                {
                  remoteConnectionEpoch: request.payload.connectionEpoch,
                  receiveSequence: request.payload.receiveSequence,
                  message: request.payload.message
                }
              )
            })
          }
        })
      }),
      { concurrency: 1, mailboxCapacity: limits.maxQueuedRpc }
    )
  }))
