import * as Automerge from "@automerge/automerge"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as InternalAutomerge from "./internal/automerge.js"
import * as Recovery from "./Recovery.js"
import * as ReplicaGate from "./ReplicaGate.js"

export interface PreparedCheckpoint {
  readonly bytes: Uint8Array
  readonly checkpointHash: string
  readonly checksum: string
  readonly commitSequence: Identity.CommitSequence
  readonly documentId: Identity.DocumentId
  readonly documentType: string
  readonly heads: ReadonlyArray<string>
}

export interface CompactResult {
  readonly checkpoint: PreparedCheckpoint
  readonly published: boolean
}

const Heads = Schema.fromJsonString(Schema.Array(Schema.String))

const CheckpointRow = Schema.Struct({
  bytes: Schema.Uint8Array,
  checkpoint_hash: Schema.String,
  checksum: Schema.String,
  commit_sequence: Identity.CommitSequence,
  heads: Heads
})

const ChangeHashRow = Schema.Struct({ change_hash: Schema.String })
const encodeHeads = Schema.encodeSync(Heads)

const DocumentRow = Schema.Struct({ document_id: Identity.DocumentId })
const CheckpointHashRow = Schema.Struct({ checkpoint_hash: Schema.String })
const CheckpointIdentity = Schema.Struct({
  bytes: Schema.Uint8Array,
  checkpointHash: Schema.String,
  checksum: Schema.String,
  commitSequence: Identity.CommitSequence,
  heads: Heads
})

export class Compaction extends Context.Service<Compaction, {
  readonly prepare: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<PreparedCheckpoint, ReplicaError.ReplicaError>
  readonly publish: (checkpoint: PreparedCheckpoint) => Effect.Effect<boolean, ReplicaError.ReplicaError>
  readonly compact: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<CompactResult, ReplicaError.ReplicaError>
  readonly prune: (documentId: Identity.DocumentId) => Effect.Effect<number, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/Compaction") {}

export const layer: Layer.Layer<
  Compaction,
  never,
  Crypto.Crypto | Recovery.Recovery | ReplicaGate.ReplicaGate | SqlClient.SqlClient
> = Layer.effect(
  Compaction,
  Effect.gen(function*() {
    const crypto = yield* Crypto.Crypto
    const digest = (value: unknown) => Canonical.digest(value).pipe(Effect.provideService(Crypto.Crypto, crypto))
    const recovery = yield* Recovery.Recovery
    const gate = yield* ReplicaGate.ReplicaGate
    const sql = yield* SqlClient.SqlClient

    const pendingCount = SqlSchema.findOneOption({
      Request: Identity.DocumentId,
      Result: Schema.Struct({ count: Schema.Int }),
      execute: (documentId) =>
        sql`SELECT COUNT(*) AS count FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 0`
    })
    const verifiedCheckpoints = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: CheckpointRow,
      execute: (documentId) =>
        sql`SELECT bytes, checkpoint_hash, checksum, commit_sequence, heads
        FROM effect_local_checkpoints
        WHERE document_id = ${documentId} AND verified = 1
        ORDER BY commit_sequence DESC, checkpoint_hash DESC`
    })
    const appliedChanges = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: ChangeHashRow,
      execute: (documentId) =>
        sql`SELECT change_hash FROM effect_local_changes
        WHERE document_id = ${documentId} AND applied = 1
        ORDER BY commit_sequence, sequence, change_hash`
    })
    const installDocumentCheckpoint = SqlSchema.findAll({
      Request: Schema.Struct({
        checkpointHash: Schema.String,
        commitSequence: Identity.CommitSequence,
        documentId: Identity.DocumentId,
        documentType: Schema.String,
        heads: Heads
      }),
      Result: DocumentRow,
      execute: ({ checkpointHash, commitSequence, documentId, documentType, heads }) =>
        sql`UPDATE effect_local_documents SET checkpoint_hash = ${checkpointHash}
          WHERE document_id = ${documentId}
            AND document_type = ${documentType}
            AND materialized_heads = ${heads}
            AND accepted_heads = ${heads}
            AND (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) = ${commitSequence}
          RETURNING document_id`
    })
    const installedCheckpoint = SqlSchema.findAll({
      Request: Schema.Struct({
        ...CheckpointIdentity.fields,
        documentId: Identity.DocumentId
      }),
      Result: CheckpointHashRow,
      execute: ({ bytes, checkpointHash, checksum, commitSequence, documentId, heads }) =>
        sql`SELECT checkpoint_hash FROM effect_local_checkpoints
          WHERE checkpoint_hash = ${checkpointHash}
            AND document_id = ${documentId}
            AND heads = ${heads}
            AND bytes = ${bytes}
            AND checksum = ${checksum}
            AND commit_sequence = ${commitSequence}
            AND verified = 1`
    })
    const retainDocumentForPrune = SqlSchema.findAll({
      Request: Schema.Struct({
        documentId: Identity.DocumentId,
        newest: CheckpointIdentity,
        oldest: CheckpointIdentity
      }),
      Result: DocumentRow,
      execute: ({ documentId, newest, oldest }) =>
        sql`UPDATE effect_local_documents SET checkpoint_hash = checkpoint_hash
          WHERE document_id = ${documentId}
            AND checkpoint_hash = ${newest.checkpointHash}
            AND materialized_heads = ${newest.heads}
            AND accepted_heads = ${newest.heads}
            AND (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) = ${newest.commitSequence}
            AND EXISTS (
              SELECT 1 FROM effect_local_checkpoints
              WHERE checkpoint_hash = ${newest.checkpointHash}
                AND bytes = ${newest.bytes}
                AND checksum = ${newest.checksum}
                AND heads = ${newest.heads}
                AND verified = 1
            )
            AND EXISTS (
              SELECT 1 FROM effect_local_checkpoints
              WHERE checkpoint_hash = ${oldest.checkpointHash}
                AND bytes = ${oldest.bytes}
                AND checksum = ${oldest.checksum}
                AND heads = ${oldest.heads}
                AND verified = 1
            )
          RETURNING document_id`
    })
    const deleteAppliedChange = SqlSchema.findAll({
      Request: Schema.Struct({
        changeHash: Schema.String,
        documentId: Identity.DocumentId
      }),
      Result: ChangeHashRow,
      execute: ({ changeHash, documentId }) =>
        sql`DELETE FROM effect_local_changes
          WHERE document_id = ${documentId} AND change_hash = ${changeHash} AND applied = 1
          RETURNING change_hash`
    })

    const prepare = <D extends Document.Any,>(document: D, documentId: Identity.DocumentId) =>
      Effect.gen(function*() {
        const stored = yield* recovery.recover(document, documentId)
        return yield* Effect.gen(function*() {
          const pending = yield* pendingCount(documentId).pipe(
            Effect.catchTags({
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause
                    })
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause
                    })
                  })
                )
            })
          )
          if (
            !Equal.equals(stored.materializedHeads, stored.acceptedHeads) ||
            Option.match(pending, { onNone: () => 0, onSome: (row) => row.count }) !== 0
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: new Error("Cannot compact an incomplete canonical history")
              })
            })
          }
          const bytes = InternalAutomerge.save(stored.automerge)
          const checksum = yield* digest(bytes)
          const checkpointHash = yield* digest({ documentId, bytes })
          return {
            bytes,
            checkpointHash,
            checksum,
            commitSequence: stored.commitSequence,
            documentId,
            documentType: document.name,
            heads: stored.materializedHeads
          }
        }).pipe(Effect.ensuring(Effect.sync(() => InternalAutomerge.free(stored.automerge))))
      })

    const publish = (checkpoint: PreparedCheckpoint) =>
      Effect.gen(function*() {
        const permit = yield* gate.current
        const checksum = yield* digest(checkpoint.bytes)
        const checkpointHash = yield* digest({ documentId: checkpoint.documentId, bytes: checkpoint.bytes })
        if (checkpoint.checkpointHash !== checkpointHash || checkpoint.checksum !== checksum) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: new Error("Prepared checkpoint checksum mismatch")
            })
          })
        }
        const verified = yield* Effect.acquireUseRelease(
          Effect.try({
            try: () => Automerge.load<InternalAutomerge.Root<unknown>>(checkpoint.bytes),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause
                })
              })
          }),
          (automerge) => Effect.sync(() => Equal.equals(Automerge.getHeads(automerge), checkpoint.heads)),
          (automerge) => Effect.sync(() => InternalAutomerge.free(automerge))
        )
        if (!verified) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: new Error("Prepared checkpoint heads mismatch")
            })
          })
        }
        return yield* sql.withTransaction(Effect.gen(function*() {
          const heads = encodeHeads(checkpoint.heads)
          const rows = yield* installDocumentCheckpoint({
            checkpointHash: checkpoint.checkpointHash,
            commitSequence: checkpoint.commitSequence,
            documentId: checkpoint.documentId,
            documentType: checkpoint.documentType,
            heads: checkpoint.heads
          })
          if (rows.length !== 1) {
            yield* gate.validate(permit)
            return false
          }
          yield* sql`INSERT INTO effect_local_checkpoints (
          checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified
        ) VALUES (
          ${checkpoint.checkpointHash}, ${checkpoint.documentId}, ${heads}, ${checkpoint.bytes},
          ${checkpoint.checksum}, ${checkpoint.commitSequence}, 1
        ) ON CONFLICT(checkpoint_hash) DO NOTHING`
          const installed = yield* installedCheckpoint({
            bytes: checkpoint.bytes,
            checkpointHash: checkpoint.checkpointHash,
            checksum: checkpoint.checksum,
            commitSequence: checkpoint.commitSequence,
            documentId: checkpoint.documentId,
            heads: checkpoint.heads
          })
          if (installed.length !== 1) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: new Error("Checkpoint identity collision")
              })
            })
          }
          const retained = yield* verifiedCheckpoints(checkpoint.documentId)
          for (const stale of retained.slice(2)) {
            yield* sql`DELETE FROM effect_local_checkpoints WHERE checkpoint_hash = ${stale.checkpoint_hash}`
          }
          yield* gate.validate(permit)
          return true
        })).pipe(
          Effect.catchTags({
            SqlError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
              )
          })
        )
      })

    const compact = <D extends Document.Any,>(document: D, documentId: Identity.DocumentId) =>
      Effect.gen(function*() {
        const checkpoint = yield* prepare(document, documentId)
        return { checkpoint, published: yield* publish(checkpoint) }
      })

    const prune = (documentId: Identity.DocumentId) =>
      Effect.gen(function*() {
        const permit = yield* gate.current
        const checkpoints = yield* verifiedCheckpoints(documentId).pipe(
          Effect.catchTags({
            SqlError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
              )
          })
        )
        if (checkpoints.length < 2) return 0
        const newest = checkpoints[0]
        const oldest = checkpoints[checkpoints.length - 1]
        return yield* Effect.scoped(Effect.gen(function*() {
          const [newestChecksum, oldestChecksum, newestHash, oldestHash] = yield* Effect.all([
            digest(newest.bytes),
            digest(oldest.bytes),
            digest({ documentId, bytes: newest.bytes }),
            digest({ documentId, bytes: oldest.bytes })
          ])
          if (
            newestChecksum !== newest.checksum || oldestChecksum !== oldest.checksum ||
            newest.checkpoint_hash !== newestHash || oldest.checkpoint_hash !== oldestHash
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: new Error("Cannot prune from a corrupt checkpoint")
              })
            })
          }
          const newestDocument = yield* Effect.acquireRelease(
            Effect.try({
              try: () => Automerge.load<InternalAutomerge.Root<unknown>>(newest.bytes),
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
            }),
            (document) => Effect.sync(() => InternalAutomerge.free(document))
          )
          const oldestDocument = yield* Effect.acquireRelease(
            Effect.try({
              try: () => Automerge.load<InternalAutomerge.Root<unknown>>(oldest.bytes),
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
            }),
            (document) => Effect.sync(() => InternalAutomerge.free(document))
          )
          if (
            !Equal.equals(Automerge.getHeads(newestDocument), newest.heads) ||
            !Equal.equals(Automerge.getHeads(oldestDocument), oldest.heads)
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: new Error("Cannot prune from checkpoint head metadata mismatch")
              })
            })
          }
          const changes = yield* appliedChanges(documentId).pipe(
            Effect.catchTags({
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause
                    })
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause
                    })
                  })
                )
            })
          )
          const dominated = changes.filter((change) =>
            Automerge.hasHeads(newestDocument, [change.change_hash]) &&
            Automerge.hasHeads(oldestDocument, [change.change_hash])
          )
          return yield* sql.withTransaction(Effect.gen(function*() {
            const rows = yield* retainDocumentForPrune({
              documentId,
              newest: {
                bytes: newest.bytes,
                checkpointHash: newest.checkpoint_hash,
                checksum: newest.checksum,
                commitSequence: newest.commit_sequence,
                heads: newest.heads
              },
              oldest: {
                bytes: oldest.bytes,
                checkpointHash: oldest.checkpoint_hash,
                checksum: oldest.checksum,
                commitSequence: oldest.commit_sequence,
                heads: oldest.heads
              }
            })
            if (rows.length !== 1) {
              yield* gate.validate(permit)
              return 0
            }
            let deleted = 0
            for (const change of dominated) {
              const removed = yield* deleteAppliedChange({ changeHash: change.change_hash, documentId })
              deleted += removed.length
            }
            yield* gate.validate(permit)
            return deleted
          })).pipe(
            Effect.catchTags({
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause
                    })
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause
                    })
                  })
                )
            })
          )
        }))
      })

    return Compaction.of({ compact, prepare, prune, publish })
  })
)
