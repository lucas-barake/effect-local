import type * as Automerge from "@automerge/automerge"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as Snapshot from "@lucas-barake/effect-local/Snapshot"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as InternalAutomerge from "./internal/automerge.js"
import * as HistoryCounters from "./internal/historyCounters.js"
import * as Rows from "./internal/rows.js"
import * as Recovery from "./Recovery.js"
import * as ReplicaGate from "./ReplicaGate.js"

const Heads = Schema.fromJsonString(Schema.Array(Schema.String))
const Versions = Schema.fromJsonString(Schema.Array(Schema.Int))

const requireAutomergeValue = (documentId: Identity.DocumentId, encoded: unknown) =>
  Document.isAutomergeValue(encoded)
    ? Effect.void
    : new ReplicaError.ReplicaError({
      reason: new ReplicaError.DocumentDecodeError({
        documentId,
        cause: new Error("Encoded value is not Automerge compatible")
      })
    })

export interface Stored<D extends Document.Any,> {
  readonly automerge: Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>
  readonly encoded: D["schema"]["Encoded"]
  readonly snapshot: Snapshot.FromDocument<D>
  readonly materializedHeads: ReadonlyArray<string>
  readonly acceptedHeads: ReadonlyArray<string>
  readonly commitSequence: Identity.CommitSequence
  readonly historyBytes: number | null
  readonly historyChanges: number | null
  readonly historyOperations: number | null
}

export class DocumentStore extends Context.Service<DocumentStore, {
  readonly create: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    value: D["schema"]["Type"]
  ) => Effect.Effect<Stored<D>, ReplicaError.ReplicaError>
  readonly load: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<Stored<D>, ReplicaError.ReplicaError>
  readonly stage: <D extends Document.Any,>(
    stored: Stored<D>,
    change: (draft: Mutation.Draft<D>) => void
  ) => Effect.Effect<Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>, ReplicaError.ReplicaError>
  readonly tombstone: <D extends Document.Any,>(
    stored: Stored<D>
  ) => Effect.Effect<Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>, ReplicaError.ReplicaError>
  readonly persist: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    durable: Stored<D>,
    staged: Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>
  ) => Effect.Effect<Stored<D>, ReplicaError.ReplicaError>
  readonly materialize: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<Stored<D>, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/DocumentStore") {}

export const layer: Layer.Layer<
  DocumentStore,
  never,
  Crypto.Crypto | ReplicaLimits.ReplicaLimits | SqlClient.SqlClient | ReplicaGate.ReplicaGate
> = Layer.effect(
  DocumentStore,
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const gate = yield* ReplicaGate.ReplicaGate
    const limits = yield* ReplicaLimits.ReplicaLimits
    const recovery = yield* Recovery.make
    const findDefinitionHash = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Schema.Struct({ definition_hash: Schema.String }),
      execute: () => sql`SELECT definition_hash FROM effect_local_metadata WHERE singleton = 1`
    })

    const nextSequence = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Schema.Struct({ commit_sequence: Identity.CommitSequence }),
      execute: () =>
        sql`UPDATE effect_local_metadata SET commit_sequence = commit_sequence + 1
          WHERE singleton = 1 RETURNING commit_sequence`
    })(undefined).pipe(
      Effect.map((row) => row.commit_sequence),
      Effect.catchTags({
        NoSuchElementError: () =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ReplicaMetadataMissing({ operation: "DocumentStore.nextSequence" })
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
    const currentDefinitionHash = findDefinitionHash(undefined).pipe(
      Effect.map((row) => row.definition_hash),
      Effect.catchTags({
        NoSuchElementError: () =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ReplicaMetadataMissing({ operation: "DocumentStore.definitionHash" })
            })
          ),
        SchemaError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
          )
      })
    )

    // Read on its own instead of as a scalar subquery inside `findPersistedDocument`. As a subquery an
    // absent singleton yielded SQL NULL, which the non-nullable row schema turned into a `SchemaError`
    // and then a per-document `StorageCorrupt`.
    const metadataCommitSequence = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Schema.Struct({ commit_sequence: Identity.CommitSequence }),
      execute: () => sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`
    })(undefined).pipe(
      Effect.map((row) => row.commit_sequence),
      Effect.catchTag("NoSuchElementError", () =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ReplicaMetadataMissing({ operation: "DocumentStore.verifyPersisted" })
          })
        ))
    )

    const findPersistedChanges = SqlSchema.findAll({
      Request: Schema.Struct({
        commitSequence: Identity.CommitSequence,
        documentId: Identity.DocumentId
      }),
      Result: Rows.ChangeRow,
      execute: (request) =>
        sql`SELECT
            actor, accepted_at, applied, bytes, change_hash, commit_sequence, dependencies,
            document_id, document_type, peer_id, sequence, writer_definition_hash, writer_schema_version
          FROM effect_local_changes
          WHERE document_id = ${request.documentId} AND commit_sequence = ${request.commitSequence}`
    })

    const findPersistedDocument = SqlSchema.findOne({
      Request: Identity.DocumentId,
      Result: Rows.DocumentRow,
      execute: (documentId) =>
        sql`SELECT
            accepted_heads, checkpoint_hash,
            document_id, document_type, history_bytes, history_changes, history_operations,
            lineage, materialized_heads, observed_versions,
            projection_status, schema_version, tombstone
          FROM effect_local_documents WHERE document_id = ${documentId}`
    })

    /**
     * Re-reads what the current `persist` just wrote and checks it round tripped.
     *
     * The recovery this replaces verified the whole retained history on every
     * write. Everything older than this commit sequence was already verified by
     * the load that opened the same transaction, so only the new rows and the
     * document row need re-reading, which keeps the check independent of history
     * length.
     */
    const verifyPersisted = <D extends Document.Any,>(options: {
      readonly changes: ReadonlyArray<InternalAutomerge.Change>
      readonly definitionHash: string
      readonly document: D
      readonly documentId: Identity.DocumentId
      readonly heads: ReadonlyArray<string>
      readonly history: HistoryCounters.HistoryCounters
      readonly sequence: Identity.CommitSequence
      readonly tombstone: boolean
    }) =>
      Effect.gen(function*() {
        const stored = yield* findPersistedChanges({
          commitSequence: options.sequence,
          documentId: options.documentId
        })
        const row = yield* findPersistedDocument(options.documentId).pipe(
          Effect.catchTag("NoSuchElementError", () =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.DocumentNotFound({ documentId: options.documentId })
              })
            ))
        )
        // After the writes, exactly where the document read-back happens: the check proves the
        // allocator still reads back the sequence this transaction allocated.
        const storedSequence = yield* metadataCommitSequence
        return yield* Effect.try({
          try: () => {
            const expected = new Set(options.changes.map((change) => change.hash))
            if (stored.length !== expected.size) {
              throw new TypeError(`Unexpected stored change count for ${options.documentId}`)
            }
            for (const change of stored) {
              const decoded = InternalAutomerge.decode(change.bytes)
              if (
                !expected.has(change.change_hash) ||
                change.applied !== 1 || change.commit_sequence !== options.sequence ||
                change.document_id !== options.documentId ||
                change.document_type !== options.document.name ||
                change.writer_schema_version !== options.document.version ||
                change.writer_definition_hash !== options.definitionHash ||
                change.peer_id !== null ||
                decoded.hash !== change.change_hash || decoded.actor !== change.actor ||
                decoded.sequence !== change.sequence ||
                Schema.encodeSync(Heads)(decoded.dependencies) !== change.dependencies
              ) throw new TypeError(`Invalid stored change: ${change.change_hash}`)
            }
            if (
              row.document_type !== options.document.name ||
              row.schema_version !== options.document.version ||
              storedSequence !== options.sequence ||
              (row.tombstone === 1) !== options.tombstone ||
              !Equal.equals(Schema.decodeUnknownSync(Heads)(row.materialized_heads), options.heads) ||
              !Equal.equals(Schema.decodeUnknownSync(Heads)(row.accepted_heads), options.heads) ||
              row.history_bytes !== options.history.bytes ||
              row.history_changes !== options.history.changes ||
              row.history_operations !== options.history.operations
            ) throw new TypeError(`Invalid stored document: ${options.documentId}`)
            // Sourced from the row rather than carried over from `durable`: the
            // recovery this replaces reported the status the document had at
            // persist time, and another writer can block it in between.
            return row.projection_status
          },
          catch: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
        })
      })

    const persist = <D extends Document.Any,>(
      document: D,
      documentId: Identity.DocumentId,
      durable: Stored<D>,
      staged: Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>
    ): Effect.Effect<Stored<D>, ReplicaError.ReplicaError> =>
      // `owned` is per run, so the finalizer below can only ever free a handle
      // this invocation forked. It stays undefined on the early return, whose
      // handle belongs to the caller.
      Effect.suspend(() => {
        let owned: Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>> | undefined
        return sql.withTransaction(Effect.gen(function*() {
          const changes = InternalAutomerge.changesSince(staged, durable.materializedHeads)
          if (changes.length === 0) return durable
          const history = yield* Effect.try({
            try: () => HistoryCounters.measure(changes),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
          }).pipe(
            Effect.flatMap((delta) =>
              HistoryCounters.add(
                {
                  bytes: durable.historyBytes,
                  changes: durable.historyChanges,
                  operations: durable.historyOperations
                },
                delta,
                limits
              )
            )
          )
          const encoded = InternalAutomerge.value(staged)
          const value = yield* Document.decode(document, documentId, encoded)
          yield* requireAutomergeValue(documentId, encoded)
          const heads = InternalAutomerge.heads(staged)
          const tombstone = InternalAutomerge.tombstone(staged)
          const sequence = yield* nextSequence
          const acceptedAt = DateTime.formatIso(yield* DateTime.now)
          const definitionHash = yield* currentDefinitionHash
          for (const change of changes) {
            yield* sql`INSERT INTO effect_local_changes (
              change_hash, document_id, document_type, writer_schema_version, writer_definition_hash,
              actor, sequence, dependencies, bytes, applied, peer_id, accepted_at, commit_sequence
            ) VALUES (
              ${change.hash}, ${documentId}, ${document.name}, ${document.version}, ${definitionHash},
              ${change.actor}, ${change.sequence}, ${Schema.encodeSync(Heads)(change.dependencies)}, ${change.bytes}, 1,
              NULL, ${acceptedAt}, ${sequence}
            )`
          }
          // Guarded on the heads `durable` observed, the same way `PeerSync` and
          // `Compaction` guard their head transitions. The recovery this replaces
          // rejected a commit whose published heads the retained history could not
          // reproduce; without the guard a stale `durable` would publish heads that
          // orphan an applied change, and no later load could recover the document.
          // A stale `durable` matches no row, so the stored heads stay behind and
          // the verification below reports them.
          yield* sql`UPDATE effect_local_documents SET
            schema_version = ${document.version},
            observed_versions = ${Schema.encodeSync(Versions)([document.version])},
            materialized_heads = ${Schema.encodeSync(Heads)(heads)},
            accepted_heads = ${Schema.encodeSync(Heads)(heads)},
            history_bytes = ${history.bytes},
            history_changes = ${history.changes},
            history_operations = ${history.operations}
            , tombstone = ${tombstone ? 1 : 0}
            WHERE document_id = ${documentId}
              AND materialized_heads = ${Schema.encodeSync(Heads)(durable.materializedHeads)}
              AND history_bytes = ${durable.historyBytes}
              AND history_changes = ${durable.historyChanges}
              AND history_operations = ${durable.historyOperations}`
          yield* sql`INSERT INTO effect_local_commit_outbox (
            commit_sequence, document_id, invalidation_keys, published
          ) VALUES (${sequence}, ${documentId}, ${Schema.encodeSync(Heads)([document.name])}, 0)`

          // The permit is sampled after the writes because that is where the
          // trailing recovery used to sample it. `materialize` holds no gate
          // lock, so an earlier sample could fence against a stale permit.
          const permit = yield* gate.current
          yield* gate.validate(permit)
          const projection = yield* verifyPersisted({
            changes,
            definitionHash,
            document,
            documentId,
            heads,
            history,
            sequence,
            tombstone
          })

          // Acquire last: nothing fallible runs between the fork and the return,
          // and the assignment shares the fork's synchronous step so an interrupt
          // cannot land between them.
          const automerge = yield* Effect.try({
            try: () => {
              owned = InternalAutomerge.clone(
                staged,
                InternalAutomerge.actorId(permit.replicaId, permit.writerGeneration, documentId)
              )
              return owned
            },
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
          })
          return {
            automerge,
            encoded,
            snapshot: {
              documentId,
              value,
              version: document.version,
              heads,
              tombstone,
              projection
            },
            materializedHeads: heads,
            acceptedHeads: heads,
            commitSequence: sequence,
            historyBytes: history.bytes,
            historyChanges: history.changes,
            historyOperations: history.operations
          }
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
                  reason: new ReplicaError.StorageCorrupt({ cause })
                })
              )
          }),
          // Outside the transaction: a failing commit is turned into a defect,
          // which the handlers above cannot observe.
          Effect.onError(() => Effect.sync(() => owned !== undefined && InternalAutomerge.free(owned)))
        )
      })

    const materialize = <D extends Document.Any,>(
      document: D,
      documentId: Identity.DocumentId
    ): Effect.Effect<Stored<D>, ReplicaError.ReplicaError> =>
      sql.withTransaction(Effect.gen(function*() {
        const durable = yield* recovery.recover(document, documentId)
        if (durable.snapshot.version === document.version) return durable
        return yield* Effect.gen(function*() {
          const encoded = yield* Document.encode(document, documentId, durable.snapshot.value)
          yield* requireAutomergeValue(documentId, encoded)
          const epoch = yield* gate.current
          const actor = InternalAutomerge.actorId(epoch.replicaId, epoch.writerGeneration, documentId)
          const staged = InternalAutomerge.stageValue(durable.automerge, actor, encoded)
          return yield* Effect.gen(function*() {
            if (InternalAutomerge.changesSince(staged, durable.materializedHeads).length === 0) {
              yield* sql`UPDATE effect_local_documents SET
                schema_version = ${document.version},
                observed_versions = ${Schema.encodeSync(Versions)([document.version])}
                WHERE document_id = ${documentId}`
              return yield* recovery.recover(document, documentId)
            }
            return yield* persist(document, documentId, durable, staged)
          }).pipe(
            Effect.ensuring(Effect.sync(() => InternalAutomerge.free(staged)))
          )
        }).pipe(
          Effect.ensuring(Effect.sync(() => InternalAutomerge.free(durable.automerge)))
        )
      })).pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause
              })
            })
          ))
      )

    return DocumentStore.of({
      create: (document, documentId, value) =>
        sql.withTransaction(Effect.gen(function*() {
          const encoded = yield* Document.encode(document, documentId, value)
          yield* requireAutomergeValue(documentId, encoded)
          const epoch = yield* gate.current
          const actor = InternalAutomerge.actorId(epoch.replicaId, epoch.writerGeneration, documentId)
          const automerge = yield* Effect.try({
            try: () => InternalAutomerge.initialize(encoded, actor),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause
                })
              })
          })
          return yield* Effect.gen(function*() {
            const heads = InternalAutomerge.heads(automerge)
            const changes = InternalAutomerge.changesSince(automerge, [])
            const history = yield* Effect.try({
              try: () => HistoryCounters.measure(changes),
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({ cause })
                })
            }).pipe(Effect.flatMap((counters) => HistoryCounters.check(counters, limits)))
            const sequence = yield* nextSequence
            const acceptedAt = DateTime.formatIso(yield* DateTime.now)
            const definitionHash = yield* currentDefinitionHash
            yield* sql`INSERT INTO effect_local_documents (
              document_id, document_type, schema_version, observed_versions,
              materialized_heads, accepted_heads, tombstone, projection_status, checkpoint_hash,
              history_changes, history_operations, history_bytes
            ) VALUES (
              ${documentId}, ${document.name}, ${document.version}, ${Schema.encodeSync(Versions)([document.version])},
              ${Schema.encodeSync(Heads)(heads)}, ${Schema.encodeSync(Heads)(heads)}, 0, 'Ready', NULL,
              ${history.changes}, ${history.operations}, ${history.bytes}
            )`
            for (const change of changes) {
              yield* sql`INSERT INTO effect_local_changes (
                change_hash, document_id, document_type, writer_schema_version, writer_definition_hash,
                actor, sequence, dependencies, bytes, applied, peer_id, accepted_at, commit_sequence
              ) VALUES (
                ${change.hash}, ${documentId}, ${document.name}, ${document.version}, ${definitionHash},
                ${change.actor}, ${change.sequence}, ${
                Schema.encodeSync(Heads)(change.dependencies)
              }, ${change.bytes}, 1,
                NULL, ${acceptedAt}, ${sequence}
              )`
            }
            yield* sql`INSERT INTO effect_local_commit_outbox (
              commit_sequence, document_id, invalidation_keys, published
            ) VALUES (${sequence}, ${documentId}, ${Schema.encodeSync(Heads)([document.name])}, 0)`
            return yield* recovery.recover(document, documentId)
          }).pipe(
            Effect.ensuring(Effect.sync(() => InternalAutomerge.free(automerge)))
          )
        })).pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause
                })
              })
            ))
        ),
      load: recovery.recover,
      stage: (stored, change) =>
        gate.current.pipe(Effect.map((epoch) =>
          InternalAutomerge.stage(
            stored.automerge,
            InternalAutomerge.actorId(epoch.replicaId, epoch.writerGeneration, stored.snapshot.documentId),
            change
          )
        )),
      tombstone: (stored) =>
        gate.current.pipe(Effect.map((epoch) =>
          InternalAutomerge.stageTombstone(
            stored.automerge,
            InternalAutomerge.actorId(epoch.replicaId, epoch.writerGeneration, stored.snapshot.documentId)
          )
        )),
      persist,
      materialize
    })
  })
)
