import * as Automerge from "@automerge/automerge"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Snapshot from "@lucas-barake/effect-local/Snapshot"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as InternalAutomerge from "./internal/automerge.js"
import * as ReplicaGate from "./ReplicaGate.js"

const DocumentRow = Schema.Struct({
  accepted_heads: Schema.String,
  checkpoint_hash: Schema.NullOr(Schema.String),
  commit_sequence: Schema.Number,
  document_id: Schema.String,
  document_type: Schema.String,
  materialized_heads: Schema.String,
  observed_versions: Schema.String,
  projection_status: Schema.Literals(["Ready", "Blocked", "Rebuilding"]),
  schema_version: Schema.Number,
  tombstone: Schema.Number
})

const CheckpointRow = Schema.Struct({
  bytes: Schema.Uint8Array,
  checkpoint_hash: Schema.String,
  checksum: Schema.String,
  commit_sequence: Schema.Number,
  document_id: Schema.String,
  heads: Schema.String,
  verified: Schema.Number
})

const ChangeRow = Schema.Struct({
  actor: Schema.String,
  accepted_at: Schema.String,
  applied: Schema.Number,
  bytes: Schema.Uint8Array,
  change_hash: Schema.String,
  commit_sequence: Schema.Number,
  dependencies: Schema.String,
  document_id: Schema.String,
  document_type: Schema.String,
  peer_id: Schema.NullOr(Schema.String),
  sequence: Schema.Number,
  writer_definition_hash: Schema.String,
  writer_schema_version: Schema.Number
})

export interface RawRecoveryExport {
  readonly document: typeof DocumentRow.Type | null
  readonly checkpoints: ReadonlyArray<typeof CheckpointRow.Type>
  readonly changes: ReadonlyArray<typeof ChangeRow.Type>
}

const Heads = Schema.fromJsonString(Schema.Array(Schema.String))
const decodeHeads = Schema.decodeUnknownSync(Heads)
const encodeHeads = Schema.encodeSync(Heads)

type RecoveredDocument<D extends Document.Any,> = {
  readonly automerge: Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>
  readonly encoded: D["schema"]["Encoded"]
  readonly snapshot: Snapshot.FromDocument<D>
  readonly materializedHeads: ReadonlyArray<string>
  readonly acceptedHeads: ReadonlyArray<string>
  readonly commitSequence: Identity.CommitSequence
}

export class Recovery extends Context.Service<Recovery, {
  readonly recover: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<RecoveredDocument<D>, ReplicaError.ReplicaError>
  readonly recoverWithPermit: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    permit: ReplicaGate.Permit
  ) => Effect.Effect<RecoveredDocument<D>, ReplicaError.ReplicaError>
  readonly exportRaw: (documentId: Identity.DocumentId) => Effect.Effect<RawRecoveryExport, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/Recovery") {}

export const make = Effect.gen(function*() {
  const crypto = yield* Crypto.Crypto
  const digest = (value: unknown) => Canonical.digest(value).pipe(Effect.provideService(Crypto.Crypto, crypto))
  const sql = yield* SqlClient.SqlClient
  const gate = yield* ReplicaGate.ReplicaGate

  const findDocument = SqlSchema.findOneOption({
    Request: Identity.DocumentId,
    Result: DocumentRow,
    execute: (documentId) =>
      sql`SELECT
          accepted_heads,
          checkpoint_hash,
          (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commit_sequence,
          document_id,
          document_type,
          materialized_heads,
          observed_versions,
          projection_status,
          schema_version,
          tombstone
        FROM effect_local_documents WHERE document_id = ${documentId}`
  })
  const findCheckpoints = SqlSchema.findAll({
    Request: Identity.DocumentId,
    Result: CheckpointRow,
    execute: (documentId) =>
      sql`SELECT bytes, checkpoint_hash, checksum, commit_sequence, document_id, heads, verified
        FROM effect_local_checkpoints
        WHERE document_id = ${documentId}
        ORDER BY commit_sequence DESC, checkpoint_hash DESC
        LIMIT 2`
  })
  const findVerifiedCheckpoints = SqlSchema.findAll({
    Request: Identity.DocumentId,
    Result: CheckpointRow,
    execute: (documentId) =>
      sql`SELECT bytes, checkpoint_hash, checksum, commit_sequence, document_id, heads, verified
        FROM effect_local_checkpoints
        WHERE document_id = ${documentId} AND verified = 1
        ORDER BY commit_sequence DESC, checkpoint_hash DESC
        LIMIT 2`
  })
  const findChanges = SqlSchema.findAll({
    Request: Identity.DocumentId,
    Result: ChangeRow,
    execute: (documentId) =>
      sql`SELECT
          actor, accepted_at, applied, bytes, change_hash, commit_sequence, dependencies,
          document_id, document_type, peer_id, sequence, writer_definition_hash, writer_schema_version
        FROM effect_local_changes
        WHERE document_id = ${documentId}
        ORDER BY commit_sequence, sequence, change_hash`
  })

  const exportRaw = (documentId: Identity.DocumentId) =>
    sql.withTransaction(Effect.gen(function*() {
      const document = yield* findDocument(documentId)
      return {
        document: document._tag === "Some" ? document.value : null,
        checkpoints: yield* findCheckpoints(documentId),
        changes: yield* findChanges(documentId)
      }
    })).pipe(
      Effect.catchTags({
        SchemaError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: new ReplicaError.SchemaCause({
                  message: String(cause),
                  path: []
                })
              })
            })
          ),
        SqlError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: new ReplicaError.SqlCause({
                  message: String(cause),
                  code: null
                })
              })
            })
          )
      })
    )

  const quarantine = (
    documentId: Identity.DocumentId,
    invalidCheckpoints: ReadonlyArray<string>,
    reason: string,
    permit: ReplicaGate.Permit
  ) =>
    sql.withTransaction(Effect.gen(function*() {
      for (const checkpointHash of invalidCheckpoints) {
        yield* sql`UPDATE effect_local_checkpoints SET verified = 0
            WHERE checkpoint_hash = ${checkpointHash}`
      }
      yield* sql`UPDATE effect_local_documents SET projection_status = 'Blocked'
          WHERE document_id = ${documentId}`
      yield* sql`DELETE FROM effect_local_quarantine
          WHERE document_id = ${documentId} AND peer_id IS NULL AND reason = ${reason}`
      yield* sql`INSERT INTO effect_local_quarantine (document_id, peer_id, reason, bytes, created_at)
          VALUES (${documentId}, NULL, ${reason}, NULL, ${DateTime.formatIso(yield* DateTime.now)})`
      yield* sql`DELETE FROM effect_local_quarantine WHERE id NOT IN (
          SELECT id FROM effect_local_quarantine ORDER BY id DESC LIMIT 1000
        )`
      yield* gate.validate(permit)
    })).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({
              cause: new ReplicaError.SqlCause({
                message: String(cause),
                code: null
              })
            })
          })
        ))
    )

  const invalidateCheckpoints = (
    invalidCheckpoints: ReadonlyArray<string>,
    permit: ReplicaGate.Permit
  ) =>
    sql.withTransaction(Effect.gen(function*() {
      for (const checkpointHash of invalidCheckpoints) {
        yield* sql`UPDATE effect_local_checkpoints SET verified = 0
            WHERE checkpoint_hash = ${checkpointHash}`
      }
      yield* gate.validate(permit)
    })).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({
              cause: new ReplicaError.SqlCause({
                message: String(cause),
                code: null
              })
            })
          })
        ))
    )

  const recoverWithPermit = <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    permit: ReplicaGate.Permit
  ) =>
    Effect.gen(function*() {
      const { changes, checkpoints, option } = yield* sql.withTransaction(Effect.gen(function*() {
        const option = yield* findDocument(documentId)
        const checkpoints = yield* findVerifiedCheckpoints(documentId)
        const changes = yield* findChanges(documentId)
        yield* gate.validate(permit)
        return { changes, checkpoints, option }
      })).pipe(
        Effect.catchTags({
          SqlError: (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: new ReplicaError.SqlCause({
                    message: String(cause),
                    code: null
                  })
                })
              })
            ),
          SchemaError: (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause: new ReplicaError.SchemaCause({
                    message: String(cause),
                    path: []
                  })
                })
              })
            )
        })
      )
      if (option._tag === "None") {
        return yield* new ReplicaError.ReplicaError({ reason: new ReplicaError.DocumentNotFound({ documentId }) })
      }
      const row = option.value
      if (row.document_type !== document.name) {
        yield* quarantine(documentId, [], `Stored document type does not match ${document.name}`, permit)
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.StorageCorrupt({
            cause: new ReplicaError.AutomergeCause({ message: `Stored document type does not match ${document.name}` })
          })
        })
      }
      if (row.schema_version > document.version) {
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.UnsupportedDocumentVersion({
            documentId,
            observedVersion: row.schema_version,
            supportedVersion: document.version
          })
        })
      }
      const actor = InternalAutomerge.actorId(permit.replicaId, permit.writerGeneration, documentId)
      const parsedHeads = yield* Effect.result(Effect.try({
        try: () => ({
          accepted: decodeHeads(row.accepted_heads),
          materialized: decodeHeads(row.materialized_heads)
        }),
        catch: (cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: new ReplicaError.AutomergeCause({ message: String(cause) })
            })
          })
      }))
      if (Result.isFailure(parsedHeads)) {
        yield* quarantine(documentId, [], "Invalid canonical head metadata", permit)
        return yield* parsedHeads.failure
      }
      const materializedHeads = parsedHeads.success.materialized
      const acceptedHeads = parsedHeads.success.accepted
      const invalidCheckpoints: Array<string> = []
      for (const checkpoint of [...checkpoints, null]) {
        let current: Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>> | undefined
        const recovered = yield* Effect.result(
          Effect.gen(function*() {
            current = yield* Effect.try({
              try: () =>
                checkpoint === null
                  ? InternalAutomerge.empty<D["schema"]["Encoded"]>(actor)
                  : InternalAutomerge.load<D["schema"]["Encoded"]>(checkpoint.bytes, actor),
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause: new ReplicaError.AutomergeCause({ message: String(cause) })
                  })
                })
            })
            if (checkpoint !== null) {
              const [checksum, checkpointHash] = yield* Effect.all([
                digest(checkpoint.bytes),
                digest({ documentId, bytes: checkpoint.bytes })
              ])
              yield* Effect.try({
                try: () => {
                  const checkpointHeads = decodeHeads(checkpoint.heads)
                  if (
                    checksum !== checkpoint.checksum || checkpoint.checkpoint_hash !== checkpointHash ||
                    !Equal.equals(InternalAutomerge.heads(current!), checkpointHeads)
                  ) throw new TypeError(`Invalid checkpoint: ${checkpoint.checkpoint_hash}`)
                },
                catch: (cause) =>
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause: new ReplicaError.AutomergeCause({ message: String(cause) })
                    })
                  })
              })
            }
            current = yield* Effect.try({
              try: () => {
                for (const change of changes) {
                  if (change.applied !== 1 || Automerge.hasHeads(current!, [change.change_hash])) continue
                  const decoded = InternalAutomerge.decode(change.bytes)
                  if (
                    decoded.hash !== change.change_hash || decoded.actor !== change.actor ||
                    decoded.sequence !== change.sequence ||
                    encodeHeads(decoded.dependencies) !== change.dependencies
                  ) throw new TypeError(`Invalid stored change: ${change.change_hash}`)
                  current = InternalAutomerge.replay(current!, [change.bytes])
                }
                if (
                  !Equal.equals(InternalAutomerge.heads(current!), materializedHeads) ||
                  !Automerge.hasHeads(current!, [...materializedHeads])
                ) throw new TypeError("Recovered Automerge heads do not match materialized heads")
                return current!
              },
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause: new ReplicaError.AutomergeCause({ message: String(cause) })
                  })
                })
            })
            return current
          }).pipe(
            Effect.onError(() =>
              Effect.sync(() => {
                if (current !== undefined) InternalAutomerge.free(current)
              })
            )
          )
        )
        if (Result.isFailure(recovered)) {
          if (checkpoint !== null) invalidCheckpoints.push(checkpoint.checkpoint_hash)
          continue
        }
        const automerge = recovered.success
        const encoded = InternalAutomerge.value(automerge)
        if (InternalAutomerge.tombstone(automerge) !== (row.tombstone === 1)) {
          InternalAutomerge.free(automerge)
          if (checkpoint !== null) invalidCheckpoints.push(checkpoint.checkpoint_hash)
          continue
        }
        const decoded = yield* Effect.result(Document.decode(document, documentId, encoded))
        if (Result.isFailure(decoded)) {
          InternalAutomerge.free(automerge)
          if (checkpoint !== null) invalidCheckpoints.push(checkpoint.checkpoint_hash)
          continue
        }
        yield* invalidateCheckpoints(invalidCheckpoints, permit).pipe(
          Effect.onError(() => Effect.sync(() => InternalAutomerge.free(automerge)))
        )
        return {
          automerge,
          encoded,
          snapshot: {
            documentId,
            value: decoded.success,
            version: row.schema_version,
            heads: materializedHeads,
            tombstone: row.tombstone === 1,
            projection: row.projection_status as Snapshot.ProjectionState
          },
          materializedHeads,
          acceptedHeads,
          commitSequence: Identity.CommitSequence.make(row.commit_sequence)
        }
      }

      yield* quarantine(documentId, invalidCheckpoints, "Canonical recovery failed", permit)
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.StorageCorrupt({
          cause: new ReplicaError.AutomergeCause({ message: `No complete verified history for document ${documentId}` })
        })
      })
    })

  const recover = <D extends Document.Any,>(document: D, documentId: Identity.DocumentId) =>
    gate.current.pipe(Effect.flatMap((permit) => recoverWithPermit(document, documentId, permit)))

  return Recovery.of({ exportRaw, recover, recoverWithPermit })
})

export const layer: Layer.Layer<Recovery, never, Crypto.Crypto | SqlClient.SqlClient | ReplicaGate.ReplicaGate> = Layer
  .effect(
    Recovery,
    make
  )
