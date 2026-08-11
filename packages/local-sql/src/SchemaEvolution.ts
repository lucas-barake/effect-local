import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Codec from "./internal/codec.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as SqlTransaction from "./internal/transaction.js"
import { MutationRuntime } from "./MutationRuntime.js"

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const NullableSchemaVersion = Schema.NullOr(Identity.SchemaVersion)
const NullableSchemaHash = Schema.NullOr(Identity.SchemaHash)
const EvolutionPhase = Schema.Literals(["Log", "Entities", "Receipts", "Pending", "Finalize"])

const MetaRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: NullableSchemaVersion,
  schema_hash: NullableSchemaHash,
  schema_generation: NonNegativeInt,
  target_schema_version: NullableSchemaVersion,
  target_schema_hash: NullableSchemaHash,
  migration_hash: NullableSchemaHash
})

const ProgressRow = Schema.Struct({
  source_schema_version: Identity.SchemaVersion,
  source_schema_hash: Identity.SchemaHash,
  target_schema_version: Identity.SchemaVersion,
  target_schema_hash: Identity.SchemaHash,
  migration_hash: Identity.SchemaHash,
  generation: Identity.SchemaVersion,
  phase: EvolutionPhase,
  cursor_model: Schema.NullOr(Schema.String),
  cursor_key: Schema.NullOr(Schema.String),
  cursor_sequence: Schema.NullOr(NonNegativeInt)
})

const EntityBatchRow = Schema.Struct({
  model: Schema.String,
  model_version: NullableSchemaVersion,
  entity_key: Schema.String,
  value_json: Schema.String
})

const LogBatchRow = Schema.Struct({
  server_sequence: Identity.ServerSequence,
  mutation_id: Identity.MutationId,
  entry_json: Schema.String,
  source_schema_version: NullableSchemaVersion,
  source_schema_hash: NullableSchemaHash
})

const ReceiptBatchRow = Schema.Struct({
  mutation_id: Identity.MutationId,
  local_sequence: Identity.LocalSequence,
  receipt_json: Schema.String,
  source_schema_version: NullableSchemaVersion,
  source_schema_hash: NullableSchemaHash,
  mutation_version: NullableSchemaVersion,
  mutation_name: Schema.NullOr(Schema.String),
  rejection_origin: Schema.NullOr(Protocol.RejectionOrigin)
})

const PendingBatchRow = Schema.Struct({
  mutation_id: Identity.MutationId,
  local_sequence: Identity.LocalSequence,
  basis: Identity.ServerSequence,
  name: Schema.String,
  payload_json: Schema.String,
  digest: Protocol.MutationDigest,
  digest_version: Protocol.MutationDigestVersion,
  source_schema_version: NullableSchemaVersion,
  source_schema_hash: NullableSchemaHash,
  mutation_version: NullableSchemaVersion,
  optimistic_result_json: Schema.String,
  changes_json: Schema.String
})

const LineageRow = Schema.Struct({ lineage_id: Schema.String })

const LegacyEntityKey = Schema.Struct({ model: Schema.String, key: Schema.Json })
const LegacyUpsert = Schema.TaggedStruct("Upsert", { entity: LegacyEntityKey, value: Schema.Json })
const LegacyDelete = Schema.TaggedStruct("Delete", { entity: LegacyEntityKey })
const LegacyEntityChange = Schema.Union([LegacyUpsert, LegacyDelete])
const LegacyAcceptedMutation = Schema.Struct({
  sequence: Identity.ServerSequence,
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: Identity.LocalSequence,
  digest: Protocol.MutationDigest,
  changes: Schema.Array(LegacyEntityChange)
})
const LegacyAcceptedReceipt = Schema.TaggedStruct("Accepted", {
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: Identity.LocalSequence,
  serverSequence: Identity.ServerSequence,
  result: Schema.Json
})
const LegacyRejectedReceipt = Schema.TaggedStruct("Rejected", {
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: Identity.LocalSequence,
  rejection: Schema.Json
})
const LegacyReceipt = Schema.Union([LegacyAcceptedReceipt, LegacyRejectedReceipt])

const sameIdentity = (left: Identity.SchemaIdentity, right: Identity.SchemaIdentity): boolean =>
  left.version === right.version && left.hash === right.hash

interface EvolutionProgress {
  readonly source_schema_version: number
  readonly source_schema_hash: string
  readonly target_schema_version: number
  readonly target_schema_hash: string
  readonly migration_hash: string
  readonly generation: number
  readonly phase: string
  readonly cursor_model: string | null
  readonly cursor_key: string | null
  readonly cursor_sequence: number | null
}

const sameProgress = (left: EvolutionProgress, right: EvolutionProgress): boolean =>
  left.source_schema_version === right.source_schema_version &&
  left.source_schema_hash === right.source_schema_hash &&
  left.target_schema_version === right.target_schema_version &&
  left.target_schema_hash === right.target_schema_hash &&
  left.migration_hash === right.migration_hash &&
  left.generation === right.generation &&
  left.phase === right.phase &&
  left.cursor_model === right.cursor_model &&
  left.cursor_key === right.cursor_key &&
  left.cursor_sequence === right.cursor_sequence

const identityFrom = (version: Identity.SchemaVersion, hash: Identity.SchemaHash): Identity.SchemaIdentity => ({
  version,
  hash
})

const decodeJson = <S extends Schema.Top,>(schema: S, encoded: string) =>
  Codec.parse(encoded).pipe(Effect.flatMap((value) => Codec.decode(schema, value)))

const sourceDefinition = (
  evolution: Evolution.Evolution,
  source: Identity.SchemaIdentity
): Effect.Effect<Definition.Any, ReplicaError.SchemaEvolutionUnsupported> => {
  const definition = evolution.definitionByIdentity.get(`${source.version}:${source.hash}`)
  return definition === undefined
    ? Effect.fail(
      new ReplicaError.SchemaEvolutionUnsupported({
        sourceVersion: source.version,
        sourceHash: source.hash,
        targetVersion: evolution.current.schemaIdentity.version,
        targetHash: evolution.current.schemaIdentity.hash
      })
    )
    : Effect.succeed(definition)
}

const resolveInitialSource = (
  meta: typeof MetaRow.Type,
  evolution: Evolution.Evolution
): Effect.Effect<
  { readonly identity: Identity.SchemaIdentity; readonly legacy: boolean },
  ReplicaError.ReplicaError
> => {
  if (meta.schema_version !== null || meta.schema_hash !== null) {
    if (meta.schema_version === null || meta.schema_hash === null) {
      return Effect.fail(new ReplicaError.StorageCorrupt({ message: "Client schema identity is partially stored" }))
    }
    return sourceDefinition(evolution, identityFrom(meta.schema_version, meta.schema_hash)).pipe(
      Effect.map((definition) => ({ identity: definition.schemaIdentity, legacy: false }))
    )
  }
  if (meta.definition_hash === evolution.current.hash) {
    return Effect.succeed({ identity: evolution.current.schemaIdentity, legacy: true })
  }
  const baseline = evolution.legacyBaselineByHash.get(meta.definition_hash)
  if (baseline === undefined) {
    return Effect.fail(
      new ReplicaError.DefinitionMismatch({
        expected: Array.from(evolution.legacyBaselineByHash.keys()).concat(evolution.current.hash).join(","),
        actual: meta.definition_hash
      })
    )
  }
  return Effect.succeed({ identity: baseline.definition.schemaIdentity, legacy: true })
}

const currentOrLegacyEntry = (
  row: typeof LogBatchRow.Type,
  source: Identity.SchemaIdentity,
  definition: Definition.Any
): Effect.Effect<Protocol.AcceptedMutation, ReplicaError.ReplicaError> =>
  Effect.gen(function*() {
    const parsed = yield* Codec.parse(row.entry_json)
    const current = yield* Codec.decode(Protocol.AcceptedMutation, parsed).pipe(Effect.result)
    if (Result.isSuccess(current)) return current.success
    const legacy = yield* Codec.decode(LegacyAcceptedMutation, parsed)
    const changes: Array<Protocol.EntityChange> = []
    for (const change of legacy.changes) {
      const model = definition.modelByName.get(change.entity.model)
      if (model === undefined) {
        return yield* new ReplicaError.StorageCorrupt({
          message: `Legacy log entry references unknown model ${change.entity.model}`
        })
      }
      changes.push(
        change._tag === "Delete"
          ? { _tag: "Delete", entity: { ...change.entity, modelVersion: model.version } }
          : { _tag: "Upsert", entity: { ...change.entity, modelVersion: model.version }, value: change.value }
      )
    }
    return {
      sequence: legacy.sequence,
      spaceId: legacy.spaceId,
      clientId: legacy.clientId,
      mutationId: legacy.mutationId,
      localSequence: legacy.localSequence,
      sourceSchema: source,
      digest: legacy.digest,
      changes
    }
  })

const currentOrLegacyReceipt = (
  row: typeof ReceiptBatchRow.Type,
  source: Identity.SchemaIdentity
): Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError> =>
  Effect.gen(function*() {
    const parsed = yield* Codec.parse(row.receipt_json)
    const current = yield* Codec.decode(Protocol.Receipt, parsed).pipe(Effect.result)
    if (Result.isSuccess(current)) return current.success
    const legacy = yield* Codec.decode(LegacyReceipt, parsed)
    return Protocol.LegacyReceipt.make({
      spaceId: legacy.spaceId,
      clientId: legacy.clientId,
      mutationId: legacy.mutationId,
      localSequence: legacy.localSequence,
      sourceSchema: source,
      outcome: legacy._tag,
      serverSequence: legacy._tag === "Accepted" ? legacy.serverSequence : null,
      body: yield* Codec.decode(Schema.Json, parsed)
    })
  })

export const migrateReceipt = (
  receipt: Protocol.Receipt,
  evolution: Evolution.Evolution
): Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError> =>
  Effect.gen(function*() {
    if (receipt._tag === "Legacy" || receipt._tag === "Expired") return receipt
    const target = evolution.current.mutationByName.get(receipt.name)
    if (target === undefined) {
      return yield* new ReplicaError.SchemaEvolutionUnsupported({
        sourceVersion: receipt.sourceSchema.version,
        sourceHash: receipt.sourceSchema.hash,
        targetVersion: evolution.current.schemaIdentity.version,
        targetHash: evolution.current.schemaIdentity.hash
      })
    }
    if (receipt._tag === "Accepted") {
      const result = yield* Evolution.migrateMutationSuccess({
        evolution,
        source: receipt.sourceSchema,
        mutation: receipt.name,
        mutationVersion: receipt.mutationVersion,
        value: receipt.result
      })
      return Protocol.AcceptedReceipt.make({
        ...receipt,
        sourceSchema: result.schemaIdentity,
        mutationVersion: result.mutationVersion,
        result: result.value
      })
    }
    if (receipt.origin !== "Mutation") {
      return Protocol.RejectedReceipt.make({
        ...receipt,
        sourceSchema: evolution.current.schemaIdentity,
        mutationVersion: target.version
      })
    }
    const rejection = yield* Evolution.migrateMutationRejection({
      evolution,
      source: receipt.sourceSchema,
      mutation: receipt.name,
      mutationVersion: receipt.mutationVersion,
      value: receipt.rejection
    })
    return Protocol.RejectedReceipt.make({
      ...receipt,
      sourceSchema: rejection.schemaIdentity,
      mutationVersion: rejection.mutationVersion,
      rejection: rejection.value
    })
  })

const pendingEnvelope = (
  row: typeof PendingBatchRow.Type,
  options: ClientOptions,
  source: Identity.SchemaIdentity,
  definition: Definition.Any
): Effect.Effect<Protocol.MutationEnvelope, ReplicaError.ReplicaError, Crypto.Crypto> =>
  Effect.gen(function*() {
    const mutation = definition.mutationByName.get(row.name)
    const rowSource = row.source_schema_version === null || row.source_schema_hash === null
      ? source
      : identityFrom(row.source_schema_version, row.source_schema_hash)
    const version = row.mutation_version ?? mutation?.version
    if (version === undefined) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Pending mutation references unknown mutation ${row.name}`
      })
    }
    const envelope = yield* Codec.decode(Protocol.MutationEnvelope, {
      spaceId: options.spaceId,
      clientId: options.clientId,
      mutationId: row.mutation_id,
      localSequence: row.local_sequence,
      basis: row.basis,
      name: row.name,
      payload: yield* decodeJson(Schema.Json, row.payload_json),
      digestVersion: row.digest_version,
      sourceSchema: rowSource,
      mutationVersion: version,
      digest: row.digest
    })
    const digest = yield* Protocol.mutationDigest({
      spaceId: envelope.spaceId,
      clientId: envelope.clientId,
      mutationId: envelope.mutationId,
      localSequence: envelope.localSequence,
      basis: envelope.basis,
      name: envelope.name,
      payload: envelope.payload,
      digestVersion: envelope.digestVersion,
      sourceSchema: envelope.sourceSchema,
      mutationVersion: envelope.mutationVersion
    })
    if (digest !== envelope.digest) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Pending mutation digest is invalid: ${row.mutation_id}`
      })
    }
    return envelope
  })

export interface ClientOptions {
  readonly definition: Definition.Any
  readonly evolution: Evolution.Evolution
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly batchSize?: number | undefined
  readonly afterBatch?: Effect.Effect<void> | undefined
}

export const client = (options: ClientOptions) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const runtime = yield* MutationRuntime
    if (!sameIdentity(options.definition.schemaIdentity, options.evolution.current.schemaIdentity)) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "evolution",
        message: "The evolution target does not match the LocalStore definition"
      })
    }
    if (
      !sameIdentity(runtime.schemaIdentity, options.evolution.current.schemaIdentity) ||
      runtime.migrationHash !== options.evolution.migrationHash
    ) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "mutationRuntime",
        message: "MutationRuntime and LocalStore must use the same evolution catalog"
      })
    }
    const batchSize = options.batchSize ?? 256
    if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > 10_000) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "schemaEvolutionBatchSize",
        message: "Schema evolution batch size must be a positive safe integer no greater than 10000"
      })
    }

    const readMeta = SqlSchema.findOne({
      Request: Schema.Void,
      Result: MetaRow,
      execute: () =>
        sql`SELECT definition_hash, schema_version, schema_hash, schema_generation,
        target_schema_version, target_schema_hash, migration_hash
        FROM effect_local_client_meta WHERE singleton = 1`
    })
    const readProgress = SqlSchema.findOneOption({
      Request: Schema.Void,
      Result: ProgressRow,
      execute: () =>
        sql`SELECT source_schema_version, source_schema_hash, target_schema_version,
        target_schema_hash, migration_hash, generation, phase, cursor_model, cursor_key, cursor_sequence
        FROM effect_local_client_evolution WHERE singleton = 1`
    })
    const beginPromotion = SqlSchema.findOneOption({
      Request: Schema.Struct({
        expectedGeneration: NonNegativeInt,
        generation: Identity.SchemaVersion,
        sourceVersion: Identity.SchemaVersion,
        sourceHash: Identity.SchemaHash
      }),
      Result: Schema.Struct({ schema_generation: Identity.SchemaVersion }),
      execute: ({ expectedGeneration, generation, sourceVersion, sourceHash }) =>
        sql`UPDATE effect_local_client_meta SET
          schema_version = ${sourceVersion}, schema_hash = ${sourceHash},
          target_schema_version = ${options.definition.schemaIdentity.version},
          target_schema_hash = ${options.definition.schemaIdentity.hash},
          migration_hash = ${options.evolution.migrationHash}, schema_generation = ${generation}
          WHERE singleton = 1 AND schema_generation = ${expectedGeneration}
            AND target_schema_version IS NULL AND target_schema_hash IS NULL AND migration_hash IS NULL
          RETURNING schema_generation`
    })
    const entityBatch = SqlSchema.findAll({
      Request: Schema.Struct({
        model: Schema.NullOr(Schema.String),
        key: Schema.NullOr(Schema.String),
        limit: Schema.Number
      }),
      Result: EntityBatchRow,
      execute: ({ model, key, limit }) =>
        sql`SELECT model, model_version, entity_key, value_json
        FROM effect_local_canonical_entities
        WHERE ${model} IS NULL OR model > ${model} OR (model = ${model} AND entity_key > ${key})
        ORDER BY model, entity_key LIMIT ${limit}`
    })
    const logBatch = SqlSchema.findAll({
      Request: Schema.Struct({ after: NonNegativeInt, limit: Schema.Number }),
      Result: LogBatchRow,
      execute: ({ after, limit }) =>
        sql`SELECT server_sequence, mutation_id, entry_json,
        source_schema_version, source_schema_hash FROM effect_local_server_log
        WHERE server_sequence > ${after} ORDER BY server_sequence LIMIT ${limit}`
    })
    const receiptBatch = SqlSchema.findAll({
      Request: Schema.Struct({ after: NonNegativeInt, limit: Schema.Number }),
      Result: ReceiptBatchRow,
      execute: ({ after, limit }) =>
        sql`SELECT mutation_id, local_sequence, receipt_json,
        source_schema_version, source_schema_hash, mutation_version, mutation_name, rejection_origin
        FROM effect_local_receipts WHERE local_sequence > ${after} ORDER BY local_sequence LIMIT ${limit}`
    })
    const pendingBatch = SqlSchema.findAll({
      Request: Schema.Struct({ after: NonNegativeInt, limit: Schema.Number }),
      Result: PendingBatchRow,
      execute: ({ after, limit }) =>
        sql`SELECT mutation_id, local_sequence, basis, name, payload_json, digest,
        digest_version, source_schema_version, source_schema_hash, mutation_version,
        optimistic_result_json, changes_json FROM effect_local_pending
        WHERE local_sequence > ${after} ORDER BY local_sequence LIMIT ${limit}`
    })
    const readLineageGroup = SqlSchema.findOneOption({
      Request: Schema.Struct({
        schemaVersion: Identity.SchemaVersion,
        schemaHash: Identity.SchemaHash,
        model: Schema.String,
        modelVersion: Identity.SchemaVersion,
        key: Schema.String
      }),
      Result: LineageRow,
      execute: (request) =>
        sql`SELECT lineage_id FROM effect_local_client_key_lineage_groups
        WHERE source_schema_version = ${request.schemaVersion} AND source_schema_hash = ${request.schemaHash}
          AND source_model = ${request.model} AND source_model_version = ${request.modelVersion}
          AND source_key = ${request.key}`
    })
    const readLineageTarget = SqlSchema.findOneOption({
      Request: Schema.Struct({ model: Schema.String, modelVersion: Identity.SchemaVersion, key: Schema.String }),
      Result: LineageRow,
      execute: (request) =>
        sql`SELECT lineage_id FROM effect_local_client_key_lineage_targets
        WHERE target_model = ${request.model} AND target_model_version = ${request.modelVersion}
          AND target_key = ${request.key}`
    })

    const registerLineage = (
      model: string,
      migrated: Evolution.MigratedModel
    ) =>
      Effect.gen(function*() {
        const aliases = yield* Effect.forEach(
          migrated.aliases,
          (alias) => Codec.stringify(alias.key).pipe(Effect.map((key) => ({ ...alias, key })))
        )
        const groups = new Set<string>()
        const sourceAliases = aliases.length === 1 ? aliases : aliases.slice(0, -1)
        for (const alias of sourceAliases) {
          const found = yield* readLineageGroup({
            schemaVersion: alias.schemaIdentity.version,
            schemaHash: alias.schemaIdentity.hash,
            model,
            modelVersion: alias.modelVersion,
            key: alias.key
          }).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isSome(found)) groups.add(found.value.lineage_id)
        }
        if (groups.size > 1) {
          return yield* new ReplicaError.SchemaKeyCollision({ model, key: yield* Codec.stringify(migrated.key) })
        }
        const root = aliases[0]
        const lineageId = groups.values().next().value ?? Canonical.stringify({
          schemaIdentity: root.schemaIdentity,
          model,
          modelVersion: root.modelVersion,
          key: root.key
        })
        const targetKey = yield* Codec.stringify(migrated.key)
        const target = yield* readLineageTarget({
          model,
          modelVersion: migrated.modelVersion,
          key: targetKey
        }).pipe(Effect.mapError(StorageUnavailable.make))
        if (Option.isSome(target) && target.value.lineage_id !== lineageId) {
          return yield* new ReplicaError.SchemaKeyCollision({ model, key: targetKey })
        }
        for (const alias of aliases) {
          yield* sql`INSERT INTO effect_local_client_key_lineage_groups
            (source_schema_version, source_schema_hash, source_model, source_model_version, source_key, lineage_id)
            VALUES (${alias.schemaIdentity.version}, ${alias.schemaIdentity.hash}, ${model},
              ${alias.modelVersion}, ${alias.key}, ${lineageId})
            ON CONFLICT (source_schema_version, source_schema_hash, source_model, source_model_version, source_key)
            DO NOTHING`
          yield* sql`INSERT INTO effect_local_client_key_lineage
            (source_schema_version, source_schema_hash, source_model, source_model_version, source_key,
              target_model, target_model_version, target_key)
            VALUES (${alias.schemaIdentity.version}, ${alias.schemaIdentity.hash}, ${model},
              ${alias.modelVersion}, ${alias.key}, ${model}, ${migrated.modelVersion}, ${targetKey})
            ON CONFLICT (source_schema_version, source_schema_hash, source_model, source_model_version, source_key)
            DO UPDATE SET target_model = excluded.target_model,
              target_model_version = excluded.target_model_version, target_key = excluded.target_key`
        }
        yield* sql`INSERT INTO effect_local_client_key_lineage_targets
          (target_model, target_model_version, target_key, lineage_id)
          VALUES (${model}, ${migrated.modelVersion}, ${targetKey}, ${lineageId})
          ON CONFLICT (target_model, target_model_version, target_key) DO NOTHING`
        const storedTarget = yield* readLineageTarget({
          model,
          modelVersion: migrated.modelVersion,
          key: targetKey
        }).pipe(Effect.mapError(StorageUnavailable.make))
        if (Option.isNone(storedTarget) || storedTarget.value.lineage_id !== lineageId) {
          return yield* new ReplicaError.SchemaKeyCollision({ model, key: targetKey })
        }
      })

    const validateBatch = (state: typeof ProgressRow.Type) =>
      Effect.gen(function*() {
        const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        const progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        if (
          meta.schema_generation !== state.generation || meta.schema_version !== state.source_schema_version ||
          meta.schema_hash !== state.source_schema_hash || meta.target_schema_version !== state.target_schema_version ||
          meta.target_schema_hash !== state.target_schema_hash || meta.migration_hash !== state.migration_hash ||
          Option.isNone(progress) || !sameProgress(progress.value, state)
        ) {
          return yield* new ReplicaError.SchemaGenerationConflict({
            expected: state.generation,
            actual: meta.schema_generation
          })
        }
      })

    let progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    if (Option.isNone(progress)) {
      const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
      const source = yield* resolveInitialSource(meta, options.evolution)
      if (!source.legacy && sameIdentity(source.identity, options.definition.schemaIdentity)) {
        return yield* sql.withTransaction(Effect.gen(function*() {
          yield* sql`UPDATE effect_local_client_meta SET definition_hash = ${options.definition.hash}
            WHERE singleton = 1 AND schema_generation = ${meta.schema_generation}
              AND target_schema_version IS NULL AND target_schema_hash IS NULL AND migration_hash IS NULL`
          const currentMeta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
          if (
            currentMeta.schema_generation !== meta.schema_generation ||
            currentMeta.schema_version !== options.definition.schemaIdentity.version ||
            currentMeta.schema_hash !== options.definition.schemaIdentity.hash ||
            currentMeta.target_schema_version !== null || currentMeta.target_schema_hash !== null ||
            currentMeta.migration_hash !== null
          ) {
            return yield* new ReplicaError.SchemaGenerationConflict({
              expected: meta.schema_generation,
              actual: currentMeta.schema_generation
            })
          }
          return currentMeta.schema_generation
        }))
      }
      if (meta.schema_generation >= Number.MAX_SAFE_INTEGER) {
        return yield* new ReplicaError.CapacityExceeded({
          resource: "schema generations",
          limit: Number.MAX_SAFE_INTEGER
        })
      }
      const generation = Identity.SchemaVersion.make(meta.schema_generation + 1)
      yield* sql.withTransaction(Effect.gen(function*() {
        const promoted = yield* beginPromotion({
          expectedGeneration: meta.schema_generation,
          generation,
          sourceVersion: source.identity.version,
          sourceHash: source.identity.hash
        }).pipe(Effect.mapError(StorageUnavailable.make))
        if (Option.isNone(promoted)) return
        yield* sql`DELETE FROM effect_local_client_shadow_entities WHERE generation = ${generation}`
        yield* sql`DELETE FROM effect_local_client_shadow_visible_entities WHERE generation = ${generation}`
        yield* sql`DELETE FROM effect_local_client_shadow_receipts_v2 WHERE generation = ${generation}`
        yield* sql`DELETE FROM effect_local_client_shadow_pending WHERE generation = ${generation}`
        yield* sql`INSERT INTO effect_local_client_evolution
          (singleton, source_schema_version, source_schema_hash, target_schema_version, target_schema_hash,
            migration_hash, generation, phase, cursor_model, cursor_key, cursor_sequence)
          VALUES (1, ${source.identity.version}, ${source.identity.hash},
            ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash},
            ${options.evolution.migrationHash}, ${generation}, 'Log', NULL, NULL, 0)`
      }))
      progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    }
    if (Option.isNone(progress)) {
      return yield* new ReplicaError.StorageCorrupt({ message: "Client schema evolution progress was not created" })
    }
    const expected = progress.value
    if (
      !sameIdentity(
        identityFrom(expected.target_schema_version, expected.target_schema_hash),
        options.definition.schemaIdentity
      ) ||
      expected.migration_hash !== options.evolution.migrationHash
    ) {
      return yield* new ReplicaError.SchemaGenerationConflict({
        expected: expected.generation,
        actual: expected.generation
      })
    }
    const source = identityFrom(expected.source_schema_version, expected.source_schema_hash)
    const definition = yield* sourceDefinition(options.evolution, source)

    while (true) {
      const current = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
      if (Option.isNone(current)) {
        const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        if (
          meta.schema_generation === expected.generation &&
          meta.schema_version === options.definition.schemaIdentity.version &&
          meta.schema_hash === options.definition.schemaIdentity.hash &&
          meta.target_schema_version === null && meta.target_schema_hash === null && meta.migration_hash === null
        ) return expected.generation
        return yield* new ReplicaError.SchemaGenerationConflict({
          expected: expected.generation,
          actual: meta.schema_generation
        })
      }
      const state = current.value
      if (state.generation !== expected.generation || state.migration_hash !== expected.migration_hash) {
        return yield* new ReplicaError.SchemaGenerationConflict({
          expected: expected.generation,
          actual: state.generation
        })
      }

      if (state.phase === "Log") {
        const rows = yield* logBatch({ after: state.cursor_sequence ?? 0, limit: batchSize }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* validateBatch(state)
          for (const row of rows) {
            const entry = yield* currentOrLegacyEntry(row, source, definition)
            for (const change of entry.changes) {
              const migrated = yield* Evolution.migrateModel({
                evolution: options.evolution,
                source: entry.sourceSchema,
                model: change.entity.model,
                modelVersion: change.entity.modelVersion,
                key: change.entity.key,
                ...(change._tag === "Upsert" ? { value: change.value } : {})
              })
              yield* registerLineage(change.entity.model, migrated)
            }
            if (row.source_schema_version === null || row.source_schema_hash === null) {
              yield* sql`UPDATE effect_local_server_log SET entry_json = ${yield* Codec.stringify(entry)},
                source_schema_version = ${entry.sourceSchema.version}, source_schema_hash = ${entry.sourceSchema.hash}
                WHERE server_sequence = ${row.server_sequence}`
            }
          }
          if (rows.length === 0) {
            yield* sql`UPDATE effect_local_client_evolution SET phase = 'Entities', cursor_sequence = NULL,
              cursor_model = NULL, cursor_key = NULL WHERE singleton = 1 AND generation = ${state.generation}`
          } else {
            yield* sql`UPDATE effect_local_client_evolution SET cursor_sequence = ${
              rows[rows.length - 1]!.server_sequence
            }
              WHERE singleton = 1 AND generation = ${state.generation}`
          }
        }))
      } else if (state.phase === "Entities") {
        const rows = yield* entityBatch({ model: state.cursor_model, key: state.cursor_key, limit: batchSize }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* validateBatch(state)
          for (const row of rows) {
            const model = definition.modelByName.get(row.model)
            const modelVersion = row.model_version ?? model?.version
            if (modelVersion === undefined) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Stored entity references unknown model ${row.model}`
              })
            }
            const migrated = yield* Evolution.migrateModel({
              evolution: options.evolution,
              source,
              model: row.model,
              modelVersion,
              key: yield* decodeJson(Schema.Json, row.entity_key),
              value: yield* decodeJson(Schema.Json, row.value_json)
            })
            yield* registerLineage(row.model, migrated)
            yield* sql`INSERT INTO effect_local_client_shadow_entities
              (generation, model, model_version, entity_key, value_json)
              VALUES (${state.generation}, ${row.model}, ${migrated.modelVersion},
                ${yield* Codec.stringify(migrated.key)}, ${yield* Codec.stringify(migrated.value)})`
          }
          if (rows.length === 0) {
            yield* sql`UPDATE effect_local_client_evolution SET phase = 'Receipts', cursor_model = NULL,
              cursor_key = NULL, cursor_sequence = 0 WHERE singleton = 1 AND generation = ${state.generation}`
          } else {
            const last = rows[rows.length - 1]!
            yield* sql`UPDATE effect_local_client_evolution SET cursor_model = ${last.model}, cursor_key = ${last.entity_key}
              WHERE singleton = 1 AND generation = ${state.generation}`
          }
        }))
      } else if (state.phase === "Receipts") {
        const rows = yield* receiptBatch({ after: state.cursor_sequence ?? 0, limit: batchSize }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* validateBatch(state)
          for (const row of rows) {
            const decoded = yield* currentOrLegacyReceipt(row, source)
            const receipt = yield* migrateReceipt(decoded, options.evolution)
            yield* sql`INSERT INTO effect_local_client_shadow_receipts_v2
              (generation, mutation_id, local_sequence, receipt_json, source_schema_version,
                source_schema_hash, mutation_version, mutation_name, rejection_origin)
              VALUES (${state.generation}, ${receipt.mutationId}, ${receipt.localSequence},
                ${yield* Codec.stringify(receipt)}, ${receipt.sourceSchema.version}, ${receipt.sourceSchema.hash},
                ${receipt._tag === "Legacy" ? null : receipt.mutationVersion},
                ${receipt._tag === "Legacy" ? null : receipt.name},
                ${receipt._tag === "Rejected" ? receipt.origin : receipt._tag === "Legacy" ? "Legacy" : null})`
          }
          if (rows.length === 0) {
            yield* sql`DELETE FROM effect_local_client_shadow_visible_entities WHERE generation = ${state.generation}`
            yield* sql`INSERT INTO effect_local_client_shadow_visible_entities
              (generation, model, model_version, entity_key, value_json)
              SELECT generation, model, model_version, entity_key, value_json
              FROM effect_local_client_shadow_entities WHERE generation = ${state.generation}`
            yield* sql`UPDATE effect_local_client_evolution SET phase = 'Pending', cursor_sequence = 0
              WHERE singleton = 1 AND generation = ${state.generation}`
          } else {
            yield* sql`UPDATE effect_local_client_evolution SET cursor_sequence = ${
              rows[rows.length - 1]!.local_sequence
            }
              WHERE singleton = 1 AND generation = ${state.generation}`
          }
        }))
      } else if (state.phase === "Pending") {
        const rows = yield* pendingBatch({ after: state.cursor_sequence ?? 0, limit: batchSize }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* validateBatch(state)
          for (const row of rows) {
            const envelope = yield* pendingEnvelope(row, options, source, definition)
            const decodedChanges = yield* decodeJson(
              Schema.Array(Protocol.EntityChange),
              row.changes_json
            ).pipe(Effect.result)
            const historicalChanges: ReadonlyArray<Protocol.EntityChange> = Result.isSuccess(decodedChanges)
              ? decodedChanges.success
              : yield* decodeJson(Schema.Array(LegacyEntityChange), row.changes_json).pipe(
                Effect.flatMap((changes) =>
                  Effect.forEach(changes, (change) => {
                    const model = definition.modelByName.get(change.entity.model)
                    return model === undefined
                      ? Effect.fail(
                        new ReplicaError.StorageCorrupt({
                          message: `Pending mutation references unknown model ${change.entity.model}`
                        })
                      )
                      : Effect.succeed(
                        change._tag === "Delete"
                          ? { _tag: "Delete" as const, entity: { ...change.entity, modelVersion: model.version } }
                          : {
                            _tag: "Upsert" as const,
                            entity: { ...change.entity, modelVersion: model.version },
                            value: change.value
                          }
                      )
                  })
                )
              )
            for (const change of historicalChanges) {
              const migrated = yield* Evolution.migrateModel({
                evolution: options.evolution,
                source: envelope.sourceSchema,
                model: change.entity.model,
                modelVersion: change.entity.modelVersion,
                key: change.entity.key,
                ...(change._tag === "Upsert" ? { value: change.value } : {})
              })
              yield* registerLineage(change.entity.model, migrated)
            }
            const changes: Array<Protocol.EntityChange> = []
            const executed = yield* runtime.executeEnvelope(
              envelope,
              SqlTransaction.local({
                sql,
                definition: options.definition,
                table: "shadow-visible",
                generation: state.generation,
                changes
              }),
              changes
            )
            if (Result.isFailure(executed)) {
              return yield* new ReplicaError.PendingMutationEvolutionRejected({
                mutationId: envelope.mutationId,
                rejection: executed.failure
              })
            }
            yield* sql`INSERT INTO effect_local_client_shadow_pending
              (generation, mutation_id, local_sequence, basis, name, payload_json, digest, digest_version,
                source_schema_version, source_schema_hash, mutation_version, optimistic_result_json, changes_json)
              VALUES (${state.generation}, ${envelope.mutationId}, ${envelope.localSequence}, ${envelope.basis},
                ${envelope.name}, ${yield* Codec.stringify(envelope.payload)}, ${envelope.digest},
                ${envelope.digestVersion}, ${envelope.sourceSchema.version}, ${envelope.sourceSchema.hash},
                ${envelope.mutationVersion}, ${yield* Codec.stringify(executed.success.result)},
                ${yield* Codec.stringify(changes)})`
          }
          if (rows.length === 0) {
            yield* sql`UPDATE effect_local_client_evolution SET phase = 'Finalize', cursor_sequence = NULL
              WHERE singleton = 1 AND generation = ${state.generation}`
          } else {
            yield* sql`UPDATE effect_local_client_evolution SET cursor_sequence = ${
              rows[rows.length - 1]!.local_sequence
            }
              WHERE singleton = 1 AND generation = ${state.generation}`
          }
        }))
      } else {
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* validateBatch(state)
          yield* sql`DELETE FROM effect_local_canonical_entities`
          yield* sql`INSERT INTO effect_local_canonical_entities (model, model_version, entity_key, value_json)
            SELECT model, model_version, entity_key, value_json FROM effect_local_client_shadow_entities
            WHERE generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_visible_entities`
          yield* sql`INSERT INTO effect_local_visible_entities (model, model_version, entity_key, value_json)
            SELECT model, model_version, entity_key, value_json FROM effect_local_client_shadow_visible_entities
            WHERE generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_receipts`
          yield* sql`INSERT INTO effect_local_receipts
            (mutation_id, local_sequence, receipt_json, source_schema_version, source_schema_hash,
              mutation_version, mutation_name, rejection_origin)
            SELECT mutation_id, local_sequence, receipt_json, source_schema_version, source_schema_hash,
              mutation_version, mutation_name, rejection_origin FROM effect_local_client_shadow_receipts_v2
            WHERE generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_pending`
          yield* sql`INSERT INTO effect_local_pending
            (mutation_id, local_sequence, basis, name, payload_json, digest, digest_version,
              source_schema_version, source_schema_hash, mutation_version, optimistic_result_json, changes_json)
            SELECT mutation_id, local_sequence, basis, name, payload_json, digest, digest_version,
              source_schema_version, source_schema_hash, mutation_version, optimistic_result_json, changes_json
              FROM effect_local_client_shadow_pending WHERE generation = ${state.generation}`
          yield* sql`UPDATE effect_local_client_meta SET definition_hash = ${options.definition.hash},
            schema_version = ${options.definition.schemaIdentity.version},
            schema_hash = ${options.definition.schemaIdentity.hash}, target_schema_version = NULL,
            target_schema_hash = NULL, migration_hash = NULL, visible_revision = visible_revision + 1
            WHERE singleton = 1 AND schema_generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_client_evolution
            WHERE singleton = 1 AND generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_client_shadow_entities WHERE generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_client_shadow_visible_entities WHERE generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_client_shadow_receipts_v2 WHERE generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_client_shadow_pending WHERE generation = ${state.generation}`
        }))
      }
      if (options.afterBatch !== undefined) yield* options.afterBatch
    }
  }).pipe(
    Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
    Effect.withSpan("SchemaEvolution.client", {
      attributes: { "space.id": options.spaceId, "client.id": options.clientId }
    })
  )

const ServerMetaEvolutionRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: NullableSchemaVersion,
  schema_hash: NullableSchemaHash,
  schema_generation: NonNegativeInt,
  target_schema_version: NullableSchemaVersion,
  target_schema_hash: NullableSchemaHash,
  migration_hash: NullableSchemaHash
})

const ServerProgressRow = Schema.Struct({
  source_schema_version: Identity.SchemaVersion,
  source_schema_hash: Identity.SchemaHash,
  target_schema_version: Identity.SchemaVersion,
  target_schema_hash: Identity.SchemaHash,
  migration_hash: Identity.SchemaHash,
  generation: Identity.SchemaVersion,
  phase: Schema.Literals(["Log", "Entities", "Receipts", "Finalize"]),
  cursor_model: Schema.NullOr(Schema.String),
  cursor_key: Schema.NullOr(Schema.String),
  cursor_sequence: Schema.NullOr(NonNegativeInt)
})

const ServerReceiptBatchRow = Schema.Struct({
  mutation_id: Identity.MutationId,
  local_sequence: Identity.LocalSequence,
  receipt_json: Schema.String,
  source_schema_version: NullableSchemaVersion,
  source_schema_hash: NullableSchemaHash,
  mutation_version: NullableSchemaVersion,
  mutation_name: Schema.NullOr(Schema.String),
  rejection_origin: Schema.NullOr(Protocol.RejectionOrigin)
})

export interface ServerOptions {
  readonly definition: Definition.Any
  readonly evolution: Evolution.Evolution
  readonly spaceId: Identity.SpaceId
  readonly batchSize?: number | undefined
  readonly afterBatch?: Effect.Effect<void> | undefined
}

export const server = (options: ServerOptions) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    if (!sameIdentity(options.definition.schemaIdentity, options.evolution.current.schemaIdentity)) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "evolution",
        message: "The evolution target does not match the ServerStore definition"
      })
    }
    const batchSize = options.batchSize ?? 256
    if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > 10_000) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "schemaEvolutionBatchSize",
        message: "Schema evolution batch size must be a positive safe integer no greater than 10000"
      })
    }

    const readMeta = SqlSchema.findOne({
      Request: Schema.Void,
      Result: ServerMetaEvolutionRow,
      execute: () =>
        sql`SELECT definition_hash, schema_version, schema_hash, schema_generation,
        target_schema_version, target_schema_hash, migration_hash FROM effect_local_server_spaces
        WHERE space_id = ${options.spaceId}`
    })
    const readProgress = SqlSchema.findOneOption({
      Request: Schema.Void,
      Result: ServerProgressRow,
      execute: () =>
        sql`SELECT source_schema_version, source_schema_hash, target_schema_version,
        target_schema_hash, migration_hash, generation, phase, cursor_model, cursor_key, cursor_sequence
        FROM effect_local_server_evolution WHERE space_id = ${options.spaceId}`
    })
    const beginPromotion = SqlSchema.findOneOption({
      Request: Schema.Struct({
        expectedGeneration: NonNegativeInt,
        generation: Identity.SchemaVersion,
        sourceVersion: Identity.SchemaVersion,
        sourceHash: Identity.SchemaHash
      }),
      Result: Schema.Struct({ schema_generation: Identity.SchemaVersion }),
      execute: ({ expectedGeneration, generation, sourceVersion, sourceHash }) =>
        sql`UPDATE effect_local_server_spaces SET
          schema_version = ${sourceVersion}, schema_hash = ${sourceHash},
          target_schema_version = ${options.definition.schemaIdentity.version},
          target_schema_hash = ${options.definition.schemaIdentity.hash},
          migration_hash = ${options.evolution.migrationHash}, schema_generation = ${generation}
          WHERE space_id = ${options.spaceId} AND schema_generation = ${expectedGeneration}
            AND target_schema_version IS NULL AND target_schema_hash IS NULL AND migration_hash IS NULL
          RETURNING schema_generation`
    })
    const logBatch = SqlSchema.findAll({
      Request: Schema.Struct({ after: NonNegativeInt, limit: Schema.Number }),
      Result: LogBatchRow,
      execute: ({ after, limit }) =>
        sql`SELECT server_sequence, mutation_id, entry_json,
        source_schema_version, source_schema_hash FROM effect_local_authoritative_log
        WHERE space_id = ${options.spaceId} AND server_sequence > ${after}
        ORDER BY server_sequence LIMIT ${limit}`
    })
    const entityBatch = SqlSchema.findAll({
      Request: Schema.Struct({
        model: Schema.NullOr(Schema.String),
        key: Schema.NullOr(Schema.String),
        limit: Schema.Number
      }),
      Result: EntityBatchRow,
      execute: ({ model, key, limit }) =>
        sql`SELECT model, model_version, entity_key, value_json
        FROM effect_local_server_entities WHERE space_id = ${options.spaceId}
          AND (${model} IS NULL OR model > ${model} OR (model = ${model} AND entity_key > ${key}))
        ORDER BY model, entity_key LIMIT ${limit}`
    })
    const receiptBatch = SqlSchema.findAll({
      Request: Schema.Struct({ after: Schema.NullOr(Schema.String), limit: Schema.Number }),
      Result: ServerReceiptBatchRow,
      execute: ({ after, limit }) =>
        sql`SELECT mutation_id, local_sequence, receipt_json,
        source_schema_version, source_schema_hash, mutation_version, mutation_name, rejection_origin
        FROM effect_local_server_receipts WHERE space_id = ${options.spaceId}
          AND (${after} IS NULL OR mutation_id > ${after}) ORDER BY mutation_id LIMIT ${limit}`
    })
    const readLineageGroup = SqlSchema.findOneOption({
      Request: Schema.Struct({
        schemaVersion: Identity.SchemaVersion,
        schemaHash: Identity.SchemaHash,
        model: Schema.String,
        modelVersion: Identity.SchemaVersion,
        key: Schema.String
      }),
      Result: LineageRow,
      execute: (request) =>
        sql`SELECT lineage_id FROM effect_local_server_key_lineage_groups
        WHERE space_id = ${options.spaceId} AND source_schema_version = ${request.schemaVersion}
          AND source_schema_hash = ${request.schemaHash} AND source_model = ${request.model}
          AND source_model_version = ${request.modelVersion} AND source_key = ${request.key}`
    })
    const readLineageTarget = SqlSchema.findOneOption({
      Request: Schema.Struct({ model: Schema.String, modelVersion: Identity.SchemaVersion, key: Schema.String }),
      Result: LineageRow,
      execute: (request) =>
        sql`SELECT lineage_id FROM effect_local_server_key_lineage_targets
        WHERE space_id = ${options.spaceId} AND target_model = ${request.model}
          AND target_model_version = ${request.modelVersion} AND target_key = ${request.key}`
    })

    const registerLineage = (model: string, migrated: Evolution.MigratedModel) =>
      Effect.gen(function*() {
        const aliases = yield* Effect.forEach(
          migrated.aliases,
          (alias) => Codec.stringify(alias.key).pipe(Effect.map((key) => ({ ...alias, key })))
        )
        const sourceAliases = aliases.length === 1 ? aliases : aliases.slice(0, -1)
        const groups = new Set<string>()
        for (const alias of sourceAliases) {
          const found = yield* readLineageGroup({
            schemaVersion: alias.schemaIdentity.version,
            schemaHash: alias.schemaIdentity.hash,
            model,
            modelVersion: alias.modelVersion,
            key: alias.key
          }).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isSome(found)) groups.add(found.value.lineage_id)
        }
        const targetKey = yield* Codec.stringify(migrated.key)
        if (groups.size > 1) return yield* new ReplicaError.SchemaKeyCollision({ model, key: targetKey })
        const root = aliases[0]
        const lineageId = groups.values().next().value ?? Canonical.stringify({
          spaceId: options.spaceId,
          schemaIdentity: root.schemaIdentity,
          model,
          modelVersion: root.modelVersion,
          key: root.key
        })
        const target = yield* readLineageTarget({
          model,
          modelVersion: migrated.modelVersion,
          key: targetKey
        }).pipe(Effect.mapError(StorageUnavailable.make))
        if (Option.isSome(target) && target.value.lineage_id !== lineageId) {
          return yield* new ReplicaError.SchemaKeyCollision({ model, key: targetKey })
        }
        for (const alias of aliases) {
          yield* sql`INSERT INTO effect_local_server_key_lineage_groups
            (space_id, source_schema_version, source_schema_hash, source_model,
              source_model_version, source_key, lineage_id)
            VALUES (${options.spaceId}, ${alias.schemaIdentity.version}, ${alias.schemaIdentity.hash},
              ${model}, ${alias.modelVersion}, ${alias.key}, ${lineageId})
            ON CONFLICT (space_id, source_schema_version, source_schema_hash, source_model,
              source_model_version, source_key) DO NOTHING`
          yield* sql`INSERT INTO effect_local_server_key_lineage
            (space_id, source_schema_version, source_schema_hash, source_model, source_model_version,
              source_key, target_model, target_model_version, target_key)
            VALUES (${options.spaceId}, ${alias.schemaIdentity.version}, ${alias.schemaIdentity.hash},
              ${model}, ${alias.modelVersion}, ${alias.key}, ${model}, ${migrated.modelVersion}, ${targetKey})
            ON CONFLICT (space_id, source_schema_version, source_schema_hash, source_model,
              source_model_version, source_key) DO UPDATE SET target_model = excluded.target_model,
              target_model_version = excluded.target_model_version, target_key = excluded.target_key`
        }
        yield* sql`INSERT INTO effect_local_server_key_lineage_targets
          (space_id, target_model, target_model_version, target_key, lineage_id)
          VALUES (${options.spaceId}, ${model}, ${migrated.modelVersion}, ${targetKey}, ${lineageId})
          ON CONFLICT (space_id, target_model, target_model_version, target_key) DO NOTHING`
        const stored = yield* readLineageTarget({
          model,
          modelVersion: migrated.modelVersion,
          key: targetKey
        }).pipe(Effect.mapError(StorageUnavailable.make))
        if (Option.isNone(stored) || stored.value.lineage_id !== lineageId) {
          return yield* new ReplicaError.SchemaKeyCollision({ model, key: targetKey })
        }
      })

    const validateBatch = (state: typeof ServerProgressRow.Type) =>
      Effect.gen(function*() {
        const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        const progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        if (
          meta.schema_generation !== state.generation || meta.schema_version !== state.source_schema_version ||
          meta.schema_hash !== state.source_schema_hash || meta.target_schema_version !== state.target_schema_version ||
          meta.target_schema_hash !== state.target_schema_hash || meta.migration_hash !== state.migration_hash ||
          Option.isNone(progress) || !sameProgress(progress.value, state)
        ) {
          return yield* new ReplicaError.SchemaGenerationConflict({
            expected: state.generation,
            actual: meta.schema_generation
          })
        }
      })

    let progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    if (Option.isNone(progress)) {
      const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
      const source = yield* resolveInitialSource(meta, options.evolution)
      if (!source.legacy && sameIdentity(source.identity, options.definition.schemaIdentity)) {
        return yield* sql.withTransaction(Effect.gen(function*() {
          yield* sql`UPDATE effect_local_server_spaces SET definition_hash = ${options.definition.hash}
            WHERE space_id = ${options.spaceId} AND schema_generation = ${meta.schema_generation}
              AND target_schema_version IS NULL AND target_schema_hash IS NULL AND migration_hash IS NULL`
          const currentMeta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
          if (
            currentMeta.schema_generation !== meta.schema_generation ||
            currentMeta.schema_version !== options.definition.schemaIdentity.version ||
            currentMeta.schema_hash !== options.definition.schemaIdentity.hash ||
            currentMeta.target_schema_version !== null || currentMeta.target_schema_hash !== null ||
            currentMeta.migration_hash !== null
          ) {
            return yield* new ReplicaError.SchemaGenerationConflict({
              expected: meta.schema_generation,
              actual: currentMeta.schema_generation
            })
          }
          return currentMeta.schema_generation
        }))
      }
      if (meta.schema_generation >= Number.MAX_SAFE_INTEGER) {
        return yield* new ReplicaError.CapacityExceeded({
          resource: "schema generations",
          limit: Number.MAX_SAFE_INTEGER
        })
      }
      const generation = Identity.SchemaVersion.make(meta.schema_generation + 1)
      yield* sql.withTransaction(Effect.gen(function*() {
        const promoted = yield* beginPromotion({
          expectedGeneration: meta.schema_generation,
          generation,
          sourceVersion: source.identity.version,
          sourceHash: source.identity.hash
        }).pipe(Effect.mapError(StorageUnavailable.make))
        if (Option.isNone(promoted)) return
        yield* sql`DELETE FROM effect_local_server_shadow_entities
          WHERE space_id = ${options.spaceId} AND generation = ${generation}`
        yield* sql`INSERT INTO effect_local_server_evolution
          (space_id, source_schema_version, source_schema_hash, target_schema_version,
            target_schema_hash, migration_hash, generation, phase, cursor_model, cursor_key, cursor_sequence)
          VALUES (${options.spaceId}, ${source.identity.version}, ${source.identity.hash},
            ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash},
            ${options.evolution.migrationHash}, ${generation}, 'Log', NULL, NULL, 0)`
      }))
      progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    }
    if (Option.isNone(progress)) {
      return yield* new ReplicaError.StorageCorrupt({ message: "Server schema evolution progress was not created" })
    }
    const expected = progress.value
    if (
      !sameIdentity(
        identityFrom(expected.target_schema_version, expected.target_schema_hash),
        options.definition.schemaIdentity
      ) ||
      expected.migration_hash !== options.evolution.migrationHash
    ) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "evolution",
        message: "The stored server promotion uses a different target or migration catalog"
      })
    }
    const source = identityFrom(expected.source_schema_version, expected.source_schema_hash)
    const definition = yield* sourceDefinition(options.evolution, source)

    while (true) {
      const current = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
      if (Option.isNone(current)) {
        const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        if (
          meta.schema_generation === expected.generation &&
          meta.schema_version === options.definition.schemaIdentity.version &&
          meta.schema_hash === options.definition.schemaIdentity.hash &&
          meta.target_schema_version === null && meta.target_schema_hash === null && meta.migration_hash === null
        ) return expected.generation
        return yield* new ReplicaError.SchemaGenerationConflict({
          expected: expected.generation,
          actual: meta.schema_generation
        })
      }
      const state = current.value
      if (state.generation !== expected.generation || state.migration_hash !== expected.migration_hash) {
        return yield* new ReplicaError.SchemaGenerationConflict({
          expected: expected.generation,
          actual: state.generation
        })
      }
      if (state.phase === "Log") {
        const rows = yield* logBatch({ after: state.cursor_sequence ?? 0, limit: batchSize }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* validateBatch(state)
          for (const row of rows) {
            const entry = yield* currentOrLegacyEntry(row, source, definition)
            for (const change of entry.changes) {
              const migrated = yield* Evolution.migrateModel({
                evolution: options.evolution,
                source: entry.sourceSchema,
                model: change.entity.model,
                modelVersion: change.entity.modelVersion,
                key: change.entity.key,
                ...(change._tag === "Upsert" ? { value: change.value } : {})
              })
              yield* registerLineage(change.entity.model, migrated)
            }
            if (row.source_schema_version === null || row.source_schema_hash === null) {
              const entryJson = yield* Codec.stringify(entry)
              yield* sql`UPDATE effect_local_authoritative_log SET entry_json = ${entryJson},
                entry_bytes = ${new TextEncoder().encode(entryJson).byteLength},
                source_schema_version = ${entry.sourceSchema.version}, source_schema_hash = ${entry.sourceSchema.hash}
                WHERE space_id = ${options.spaceId} AND server_sequence = ${row.server_sequence}`
            }
          }
          if (rows.length === 0) {
            yield* sql`UPDATE effect_local_server_evolution SET phase = 'Entities', cursor_sequence = NULL,
              cursor_model = NULL, cursor_key = NULL WHERE space_id = ${options.spaceId}
              AND generation = ${state.generation}`
          } else {
            yield* sql`UPDATE effect_local_server_evolution SET cursor_sequence = ${
              rows[rows.length - 1]!.server_sequence
            }
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
          }
        }))
      } else if (state.phase === "Entities") {
        const rows = yield* entityBatch({ model: state.cursor_model, key: state.cursor_key, limit: batchSize }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* validateBatch(state)
          for (const row of rows) {
            const model = definition.modelByName.get(row.model)
            const modelVersion = row.model_version ?? model?.version
            if (modelVersion === undefined) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Stored entity references unknown model ${row.model}`
              })
            }
            const migrated = yield* Evolution.migrateModel({
              evolution: options.evolution,
              source,
              model: row.model,
              modelVersion,
              key: yield* decodeJson(Schema.Json, row.entity_key),
              value: yield* decodeJson(Schema.Json, row.value_json)
            })
            yield* registerLineage(row.model, migrated)
            yield* sql`INSERT INTO effect_local_server_shadow_entities
              (space_id, generation, model, model_version, entity_key, value_json)
              VALUES (${options.spaceId}, ${state.generation}, ${row.model}, ${migrated.modelVersion},
                ${yield* Codec.stringify(migrated.key)}, ${yield* Codec.stringify(migrated.value)})`
          }
          if (rows.length === 0) {
            yield* sql`UPDATE effect_local_server_evolution SET phase = 'Receipts', cursor_model = NULL,
              cursor_key = NULL, cursor_sequence = 0 WHERE space_id = ${options.spaceId}
              AND generation = ${state.generation}`
          } else {
            const last = rows[rows.length - 1]!
            yield* sql`UPDATE effect_local_server_evolution SET cursor_model = ${last.model}, cursor_key = ${last.entity_key}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
          }
        }))
      } else if (state.phase === "Receipts") {
        const rows = yield* receiptBatch({ after: state.cursor_key, limit: batchSize }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* validateBatch(state)
          for (const row of rows) {
            const receipt = yield* currentOrLegacyReceipt(row, source)
            yield* sql`UPDATE effect_local_server_receipts SET receipt_json = ${yield* Codec.stringify(receipt)},
              source_schema_version = ${receipt.sourceSchema.version}, source_schema_hash = ${receipt.sourceSchema.hash},
              mutation_version = ${receipt._tag === "Legacy" ? null : receipt.mutationVersion},
              mutation_name = ${receipt._tag === "Legacy" ? null : receipt.name},
              rejection_origin = ${
              receipt._tag === "Rejected" ? receipt.origin : receipt._tag === "Legacy" ? "Legacy" : null
            }
              WHERE space_id = ${options.spaceId} AND mutation_id = ${row.mutation_id}`
          }
          if (rows.length === 0) {
            yield* sql`UPDATE effect_local_server_evolution SET phase = 'Finalize', cursor_key = NULL,
              cursor_sequence = NULL
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
          } else {
            yield* sql`UPDATE effect_local_server_evolution SET cursor_key = ${rows[rows.length - 1]!.mutation_id}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
          }
        }))
      } else {
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* validateBatch(state)
          yield* sql`DELETE FROM effect_local_server_entities WHERE space_id = ${options.spaceId}`
          yield* sql`INSERT INTO effect_local_server_entities
            (space_id, model, model_version, entity_key, value_json)
            SELECT space_id, model, model_version, entity_key, value_json
            FROM effect_local_server_shadow_entities
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
          yield* sql`UPDATE effect_local_server_spaces SET definition_hash = ${options.definition.hash},
            schema_version = ${options.definition.schemaIdentity.version},
            schema_hash = ${options.definition.schemaIdentity.hash}, target_schema_version = NULL,
            target_schema_hash = NULL, migration_hash = NULL
            WHERE space_id = ${options.spaceId} AND schema_generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_server_evolution
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
          yield* sql`DELETE FROM effect_local_server_shadow_entities
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }))
      }
      if (options.afterBatch !== undefined) yield* options.afterBatch
    }
  }).pipe(
    Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
    Effect.withSpan("SchemaEvolution.server", { attributes: { "space.id": options.spaceId } })
  )
