import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as ClientLineage from "./internal/clientLineage.js"
import * as Codec from "./internal/codec.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as TerminalRejection from "./internal/TerminalRejection.js"
import * as SqlTransaction from "./internal/transaction.js"
import { MutationRuntime } from "./MutationRuntime.js"
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const NullableSchemaVersion = Schema.NullOr(Identity.SchemaVersion)
const NullableSchemaHash = Schema.NullOr(Identity.SchemaHash)
const EvolutionPhase = Schema.Literals([
  "Log",
  "Entities",
  "Receipts",
  "Pending",
  "Retractions",
  "Flip",
  "CleanupCanonical",
  "CleanupVisible",
  "CleanupRetractions",
  "CleanupReceipts",
  "CleanupPending",
  "Finalize"
])

const MetaRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: NullableSchemaVersion,
  schema_hash: NullableSchemaHash,
  schema_generation: NonNegativeInt,
  active_schema_generation: NonNegativeInt,
  active_projection_generation: NonNegativeInt.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
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
  source_generation: NonNegativeInt,
  source_projection_generation: NonNegativeInt,
  target_projection_generation: NonNegativeInt,
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

const RetractionBatchRow = Schema.Struct({
  model: Schema.String,
  model_version: Identity.SchemaVersion,
  entity_key: Schema.String
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
  membership_incarnation: Identity.MembershipIncarnation,
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
  changes_json: Schema.String,
  submission_state: Protocol.SubmissionState,
  attempt_count: NonNegativeInt,
  receipt_count: NonNegativeInt
})

const SequenceBytesRow = Schema.Struct({
  server_sequence: Identity.ServerSequence,
  row_bytes: NonNegativeInt
})
const LocalSequenceBytesRow = Schema.Struct({
  local_sequence: Identity.LocalSequence,
  row_bytes: NonNegativeInt
})
const EntityBytesRow = Schema.Struct({
  model: Schema.String,
  entity_key: Schema.String,
  row_bytes: NonNegativeInt
})
const KeyBytesRow = Schema.Struct({
  mutation_id: Identity.MutationId,
  row_bytes: NonNegativeInt
})
const CountRow = Schema.Struct({ count: NonNegativeInt })

const boundedCount = (
  rows: ReadonlyArray<{ readonly row_bytes: number }>,
  batchBytes: number
): Effect.Effect<number, ReplicaError.CapacityExceeded> => {
  if (rows.length === 0) return Effect.succeed(0)
  if (rows[0].row_bytes > batchBytes) {
    return Effect.fail(
      new ReplicaError.CapacityExceeded({
        resource: "schema evolution row bytes",
        limit: batchBytes
      })
    )
  }
  let total = 0
  let count = 0
  for (const row of rows) {
    if (total + row.row_bytes > batchBytes) break
    total += row.row_bytes
    count += 1
  }
  return Effect.succeed(count)
}

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
  readonly source_generation: number
  readonly source_projection_generation?: number
  readonly target_projection_generation?: number
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
  left.source_generation === right.source_generation &&
  left.source_projection_generation === right.source_projection_generation &&
  left.target_projection_generation === right.target_projection_generation &&
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

const migrateEntityChange = (
  evolution: Evolution.Evolution,
  source: Identity.SchemaIdentity,
  change: Protocol.EntityChange
) => {
  const request = {
    evolution,
    source,
    model: change.entity.model,
    modelVersion: change.entity.modelVersion,
    key: change.entity.key
  }
  if (change._tag === "Upsert") return Evolution.migrateModel({ ...request, value: change.value })
  return Evolution.migrateModel(request)
}

const protocolReceiptMetadata = (receipt: Protocol.Receipt) => {
  if (receipt._tag === "Legacy") {
    return { mutationVersion: null, mutationName: null, rejectionOrigin: "Legacy" }
  }
  let rejectionOrigin: string | null = null
  if (receipt._tag === "Rejected") rejectionOrigin = receipt.origin
  return {
    mutationVersion: receipt.mutationVersion,
    mutationName: receipt.name,
    rejectionOrigin
  }
}

const sourceDefinition = (
  evolution: Evolution.Evolution,
  source: Identity.SchemaIdentity
): Effect.Effect<Definition.Any, ReplicaError.SchemaEvolutionUnsupported> => {
  const definition = evolution.definitionByIdentity.get(`${source.version}:${source.hash}`)
  if (definition === undefined) {
    return Effect.fail(
      new ReplicaError.SchemaEvolutionUnsupported({
        sourceVersion: source.version,
        sourceHash: source.hash,
        targetVersion: evolution.current.schemaIdentity.version,
        targetHash: evolution.current.schemaIdentity.hash
      })
    )
  }
  return Effect.succeed(definition)
}

const resolveInitialSource = (
  meta: Pick<typeof MetaRow.Type, "definition_hash" | "schema_version" | "schema_hash">,
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

const currentOrLegacyEntry = Effect.fnUntraced(function*(
  row: typeof LogBatchRow.Type,
  source: Identity.SchemaIdentity,
  definition: Definition.Any
) {
  const parsed = yield* Codec.parse(row.entry_json)
  const current = yield* Codec.decode(Protocol.AcceptedMutation, parsed).pipe(Effect.result)
  if (Result.isSuccess(current)) {
    const entry = current.success
    const incompleteSource = (row.source_schema_version === null) !== (row.source_schema_hash === null)
    if (
      entry.sequence !== row.server_sequence || entry.mutationId !== row.mutation_id || incompleteSource ||
      (row.source_schema_version !== null && row.source_schema_version !== entry.sourceSchema.version) ||
      (row.source_schema_hash !== null && row.source_schema_hash !== entry.sourceSchema.hash)
    ) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Accepted entry ${row.server_sequence} conflicts with its durable metadata`
      })
    }
    return entry
  }
  const legacy = yield* Codec.decode(LegacyAcceptedMutation, parsed)
  const incompleteSource = (row.source_schema_version === null) !== (row.source_schema_hash === null)
  if (
    legacy.sequence !== row.server_sequence || legacy.mutationId !== row.mutation_id || incompleteSource ||
    (row.source_schema_version !== null && row.source_schema_version !== source.version) ||
    (row.source_schema_hash !== null && row.source_schema_hash !== source.hash)
  ) {
    return yield* new ReplicaError.StorageCorrupt({
      message: `Legacy accepted entry ${row.server_sequence} conflicts with its durable metadata`
    })
  }
  const changes: Array<Protocol.EntityChange> = []
  for (const change of legacy.changes) {
    const model = definition.modelByName.get(change.entity.model)
    if (model === undefined) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Legacy log entry references unknown model ${change.entity.model}`
      })
    }
    if (change._tag === "Delete") {
      changes.push({ _tag: "Delete", entity: { ...change.entity, modelVersion: model.version } })
    } else {
      changes.push({ _tag: "Upsert", entity: { ...change.entity, modelVersion: model.version }, value: change.value })
    }
  }
  return {
    sequence: legacy.sequence,
    spaceId: legacy.spaceId,
    clientId: legacy.clientId,
    membershipIncarnation: Identity.legacyMembershipIncarnation,
    mutationId: legacy.mutationId,
    localSequence: legacy.localSequence,
    sourceSchema: source,
    digest: legacy.digest,
    changes
  }
})

const currentOrLegacyReceipt = Effect.fnUntraced(function*(
  row: typeof ReceiptBatchRow.Type,
  source: Identity.SchemaIdentity
) {
  const parsed = yield* Codec.parse(row.receipt_json)
  const current = yield* Codec.decode(Protocol.Receipt, parsed).pipe(Effect.result)
  if (Result.isSuccess(current)) {
    const receipt = current.success
    const incompleteSource = (row.source_schema_version === null) !== (row.source_schema_hash === null)
    const expected = protocolReceiptMetadata(receipt)
    if (
      receipt.mutationId !== row.mutation_id || receipt.localSequence !== row.local_sequence || incompleteSource ||
      (row.source_schema_version !== null && row.source_schema_version !== receipt.sourceSchema.version) ||
      (row.source_schema_hash !== null && row.source_schema_hash !== receipt.sourceSchema.hash) ||
      (row.mutation_version !== null && row.mutation_version !== expected.mutationVersion) ||
      (row.mutation_name !== null && row.mutation_name !== expected.mutationName) ||
      (row.rejection_origin !== null && row.rejection_origin !== expected.rejectionOrigin)
    ) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Receipt ${row.mutation_id} conflicts with its durable metadata`
      })
    }
    return receipt
  }
  const legacy = yield* Codec.decode(LegacyReceipt, parsed)
  const incompleteSource = (row.source_schema_version === null) !== (row.source_schema_hash === null)
  if (
    legacy.mutationId !== row.mutation_id || legacy.localSequence !== row.local_sequence || incompleteSource ||
    (row.source_schema_version !== null && row.source_schema_version !== source.version) ||
    (row.source_schema_hash !== null && row.source_schema_hash !== source.hash) ||
    row.mutation_version !== null || row.mutation_name !== null ||
    (row.rejection_origin !== null && row.rejection_origin !== "Legacy")
  ) {
    return yield* new ReplicaError.StorageCorrupt({
      message: `Legacy receipt ${row.mutation_id} conflicts with its durable metadata`
    })
  }
  let serverSequence: Identity.ServerSequence | null = null
  if (legacy._tag === "Accepted") serverSequence = legacy.serverSequence
  return Protocol.LegacyReceipt.make({
    spaceId: legacy.spaceId,
    clientId: legacy.clientId,
    membershipIncarnation: Identity.legacyMembershipIncarnation,
    mutationId: legacy.mutationId,
    localSequence: legacy.localSequence,
    sourceSchema: source,
    outcome: legacy._tag,
    serverSequence,
    body: yield* Codec.decode(Schema.Json, parsed)
  })
})

export const migrateReceipt = Effect.fnUntraced(function*(
  receipt: Protocol.Receipt,
  evolution: Evolution.Evolution
) {
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

const pendingEnvelope = Effect.fnUntraced(function*(
  row: typeof PendingBatchRow.Type,
  options: ClientOptions,
  source: Identity.SchemaIdentity,
  definition: Definition.Any
) {
  const mutation = definition.mutationByName.get(row.name)
  let rowSource = source
  if (row.source_schema_version !== null && row.source_schema_hash !== null) {
    rowSource = identityFrom(row.source_schema_version, row.source_schema_hash)
  }
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
    membershipIncarnation: row.membership_incarnation,
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
    membershipIncarnation: envelope.membershipIncarnation,
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
  readonly batchBytes?: number | undefined
  readonly afterBatch?: Effect.Effect<void> | undefined
}

export const client = Effect.fn("SchemaEvolution.client")(function*(options: ClientOptions) {
  yield* Effect.annotateCurrentSpan({ "space.id": options.spaceId, "client.id": options.clientId })
  const sql = yield* SqlClient.SqlClient
  const withTransaction = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
    sql.withTransaction(effect)
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
  const batchBytes = options.batchBytes ?? Protocol.maximumBatchBytes
  if (!Number.isSafeInteger(batchBytes) || batchBytes < 1) {
    return yield* new ReplicaError.InvalidConfiguration({
      option: "schemaEvolutionBatchBytes",
      message: "Schema evolution batch bytes must be a positive safe integer"
    })
  }

  const readMeta = SqlSchema.findOne({
    Request: Schema.Void,
    Result: MetaRow,
    execute: () =>
      sql`SELECT definition_hash, schema_version, schema_hash, schema_generation, active_schema_generation,
        active_projection_generation,
        target_schema_version, target_schema_hash, migration_hash
        FROM effect_local_client_spaces WHERE space_id = ${options.spaceId}`
  })
  const readProgress = SqlSchema.findOneOption({
    Request: Schema.Void,
    Result: ProgressRow,
    execute: () =>
      sql`SELECT source_schema_version, source_schema_hash, target_schema_version,
        target_schema_hash, migration_hash, generation, source_generation, source_projection_generation,
        target_projection_generation, phase,
        cursor_model, cursor_key, cursor_sequence
        FROM effect_local_client_evolution WHERE space_id = ${options.spaceId}`
  })
  const countCanonicalGeneration = SqlSchema.findOne({
    Request: NonNegativeInt,
    Result: CountRow,
    execute: (generation) =>
      sql`SELECT COUNT(*) AS count FROM effect_local_client_canonical_entities_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}`
  })
  const countVisibleGeneration = SqlSchema.findOne({
    Request: NonNegativeInt,
    Result: CountRow,
    execute: (generation) =>
      sql`SELECT COUNT(*) AS count FROM effect_local_client_visible_entities_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}`
  })
  const countReceiptGeneration = SqlSchema.findOne({
    Request: NonNegativeInt,
    Result: CountRow,
    execute: (generation) =>
      sql`SELECT COUNT(*) AS count FROM effect_local_client_receipts_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}`
  })
  const countPendingGeneration = SqlSchema.findOne({
    Request: NonNegativeInt,
    Result: CountRow,
    execute: (generation) =>
      sql`SELECT COUNT(*) AS count FROM effect_local_client_pending_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}`
  })
  const countRetractionGeneration = SqlSchema.findOne({
    Request: NonNegativeInt,
    Result: CountRow,
    execute: (generation) =>
      sql`SELECT COUNT(*) AS count FROM effect_local_client_retractions
        WHERE space_id = ${options.spaceId} AND generation = ${generation}`
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
      sql`UPDATE effect_local_client_spaces SET
          schema_version = ${sourceVersion}, schema_hash = ${sourceHash},
          target_schema_version = ${options.definition.schemaIdentity.version},
          target_schema_hash = ${options.definition.schemaIdentity.hash},
          migration_hash = ${options.evolution.migrationHash}, schema_generation = ${generation}
          WHERE space_id = ${options.spaceId} AND schema_generation = ${expectedGeneration}
            AND target_schema_version IS NULL AND target_schema_hash IS NULL AND migration_hash IS NULL
          RETURNING schema_generation`
  })
  const initialEntityMetadata = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, limit: Schema.Number }),
    Result: EntityBytesRow,
    execute: ({ generation, limit }) =>
      sql`SELECT model, entity_key,
          length(CAST(entity_key AS BLOB)) + length(CAST(value_json AS BLOB)) AS row_bytes
        FROM effect_local_client_canonical_entities_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const continuingEntityMetadata = SqlSchema.findAll({
    Request: Schema.Struct({
      generation: NonNegativeInt,
      model: Schema.String,
      key: Schema.String,
      limit: Schema.Number
    }),
    Result: EntityBytesRow,
    execute: ({ generation, model, key, limit }) =>
      sql`SELECT model, entity_key,
          length(CAST(entity_key AS BLOB)) + length(CAST(value_json AS BLOB)) AS row_bytes
        FROM effect_local_client_canonical_entities_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}
          AND (model > ${model} OR (model = ${model} AND entity_key > ${key}))
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const initialEntityBatch = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, limit: Schema.Number }),
    Result: EntityBatchRow,
    execute: ({ generation, limit }) =>
      sql`SELECT model, model_version, entity_key, value_json
        FROM effect_local_client_canonical_entities_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const continuingEntityBatch = SqlSchema.findAll({
    Request: Schema.Struct({
      generation: NonNegativeInt,
      model: Schema.String,
      key: Schema.String,
      limit: Schema.Number
    }),
    Result: EntityBatchRow,
    execute: ({ generation, model, key, limit }) =>
      sql`SELECT model, model_version, entity_key, value_json
        FROM effect_local_client_canonical_entities_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}
          AND (model > ${model} OR (model = ${model} AND entity_key > ${key}))
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const initialRetractionMetadata = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, limit: Schema.Number }),
    Result: EntityBytesRow,
    execute: ({ generation, limit }) =>
      sql`SELECT model, entity_key, length(CAST(entity_key AS BLOB)) AS row_bytes
        FROM effect_local_client_retractions
        WHERE space_id = ${options.spaceId} AND generation = ${generation}
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const continuingRetractionMetadata = SqlSchema.findAll({
    Request: Schema.Struct({
      generation: NonNegativeInt,
      model: Schema.String,
      key: Schema.String,
      limit: Schema.Number
    }),
    Result: EntityBytesRow,
    execute: ({ generation, model, key, limit }) =>
      sql`SELECT model, entity_key, length(CAST(entity_key AS BLOB)) AS row_bytes
        FROM effect_local_client_retractions
        WHERE space_id = ${options.spaceId} AND generation = ${generation}
          AND (model > ${model} OR (model = ${model} AND entity_key > ${key}))
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const initialRetractionBatch = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, limit: Schema.Number }),
    Result: RetractionBatchRow,
    execute: ({ generation, limit }) =>
      sql`SELECT model, model_version, entity_key FROM effect_local_client_retractions
        WHERE space_id = ${options.spaceId} AND generation = ${generation}
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const continuingRetractionBatch = SqlSchema.findAll({
    Request: Schema.Struct({
      generation: NonNegativeInt,
      model: Schema.String,
      key: Schema.String,
      limit: Schema.Number
    }),
    Result: RetractionBatchRow,
    execute: ({ generation, model, key, limit }) =>
      sql`SELECT model, model_version, entity_key FROM effect_local_client_retractions
        WHERE space_id = ${options.spaceId} AND generation = ${generation}
          AND (model > ${model} OR (model = ${model} AND entity_key > ${key}))
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const logMetadata = SqlSchema.findAll({
    Request: Schema.Struct({ after: NonNegativeInt, limit: Schema.Number }),
    Result: SequenceBytesRow,
    execute: ({ after, limit }) =>
      sql`SELECT server_sequence, length(CAST(entry_json AS BLOB)) AS row_bytes
        FROM effect_local_server_log WHERE space_id = ${options.spaceId} AND server_sequence > ${after}
        ORDER BY server_sequence LIMIT ${limit}`
  })
  const logBatch = SqlSchema.findAll({
    Request: Schema.Struct({ after: NonNegativeInt, limit: Schema.Number }),
    Result: LogBatchRow,
    execute: ({ after, limit }) =>
      sql`SELECT server_sequence, mutation_id, entry_json,
        source_schema_version, source_schema_hash FROM effect_local_server_log
        WHERE space_id = ${options.spaceId} AND server_sequence > ${after}
        ORDER BY server_sequence LIMIT ${limit}`
  })
  const receiptBatch = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, after: NonNegativeInt, limit: Schema.Number }),
    Result: ReceiptBatchRow,
    execute: ({ generation, after, limit }) =>
      sql`SELECT mutation_id, local_sequence, receipt_json,
        source_schema_version, source_schema_hash, mutation_version, mutation_name, rejection_origin
        FROM effect_local_client_receipts_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}
          AND local_sequence > ${after} ORDER BY local_sequence LIMIT ${limit}`
  })
  const receiptMetadata = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, after: NonNegativeInt, limit: Schema.Number }),
    Result: LocalSequenceBytesRow,
    execute: ({ generation, after, limit }) =>
      sql`SELECT local_sequence, length(CAST(receipt_json AS BLOB)) AS row_bytes
        FROM effect_local_client_receipts_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}
          AND local_sequence > ${after} ORDER BY local_sequence LIMIT ${limit}`
  })
  const pendingBatch = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, after: NonNegativeInt, limit: Schema.Number }),
    Result: PendingBatchRow,
    execute: ({ generation, after, limit }) =>
      sql`SELECT membership_incarnation, mutation_id, local_sequence, basis, name, payload_json, digest,
        digest_version, source_schema_version, source_schema_hash, mutation_version,
        optimistic_result_json, changes_json, submission_state, attempt_count,
        (SELECT COUNT(*) FROM effect_local_client_receipts_data AS r
          WHERE r.space_id = p.space_id AND r.schema_generation = p.schema_generation
            AND r.mutation_id = p.mutation_id) AS receipt_count
        FROM effect_local_client_pending_data AS p
        WHERE p.space_id = ${options.spaceId} AND p.schema_generation = ${generation}
          AND p.local_sequence > ${after}
        ORDER BY local_sequence LIMIT ${limit}`
  })
  const pendingMetadata = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, after: NonNegativeInt, limit: Schema.Number }),
    Result: LocalSequenceBytesRow,
    execute: ({ generation, after, limit }) =>
      sql`SELECT local_sequence, length(CAST(payload_json AS BLOB)) +
          length(CAST(optimistic_result_json AS BLOB)) + length(CAST(changes_json AS BLOB)) AS row_bytes
        FROM effect_local_client_pending_data
        WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}
          AND local_sequence > ${after} ORDER BY local_sequence LIMIT ${limit}`
  })
  const registerLineage = ClientLineage.make(sql, options.spaceId)

  const validateBatch = Effect.fnUntraced(function*(state: typeof ProgressRow.Type) {
    const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    const progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    let expectedActiveGeneration: number = state.generation
    if (
      state.phase === "Flip" || state.phase === "Log" || state.phase === "Entities" ||
      state.phase === "Receipts" || state.phase === "Pending" || state.phase === "Retractions"
    ) {
      expectedActiveGeneration = state.source_generation
    }
    if (
      meta.schema_generation !== state.generation || meta.schema_version !== state.source_schema_version ||
      meta.active_schema_generation !== expectedActiveGeneration ||
      meta.schema_hash !== state.source_schema_hash || meta.target_schema_version !== state.target_schema_version ||
      meta.target_schema_hash !== state.target_schema_hash || meta.migration_hash !== state.migration_hash ||
      Option.isNone(progress) || !sameProgress(progress.value, state)
    ) {
      return yield* new ReplicaError.SchemaGenerationConflict({
        expected: state.generation,
        actual: meta.schema_generation
      })
    }
    return undefined
  })

  let progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
  if (Option.isNone(progress)) {
    const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    const source = yield* resolveInitialSource(meta, options.evolution)
    if (!source.legacy && sameIdentity(source.identity, options.definition.schemaIdentity)) {
      if (meta.definition_hash === options.definition.hash) return meta.schema_generation
      return yield* withTransaction(Effect.gen(function*() {
        yield* sql`UPDATE effect_local_client_spaces SET definition_hash = ${options.definition.hash}
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
    yield* withTransaction(Effect.gen(function*() {
      const promoted = yield* beginPromotion({
        expectedGeneration: meta.schema_generation,
        generation,
        sourceVersion: source.identity.version,
        sourceHash: source.identity.hash
      }).pipe(Effect.mapError(StorageUnavailable.make))
      if (Option.isNone(promoted)) return
      yield* sql`DELETE FROM effect_local_client_canonical_entities_data
          WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}`
      yield* sql`DELETE FROM effect_local_client_visible_entities_data
          WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}`
      yield* sql`DELETE FROM effect_local_client_receipts_data
          WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}`
      yield* sql`DELETE FROM effect_local_client_pending_data
          WHERE space_id = ${options.spaceId} AND schema_generation = ${generation}`
      yield* sql`DELETE FROM effect_local_client_retractions
          WHERE space_id = ${options.spaceId} AND generation = ${generation}`
      yield* sql`INSERT INTO effect_local_client_evolution
          (space_id, source_schema_version, source_schema_hash, target_schema_version, target_schema_hash,
            migration_hash, generation, source_generation, source_projection_generation,
            target_projection_generation, phase, cursor_model, cursor_key, cursor_sequence)
          VALUES (${options.spaceId}, ${source.identity.version}, ${source.identity.hash},
            ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash},
            ${options.evolution.migrationHash}, ${generation}, ${meta.active_schema_generation},
            ${meta.active_projection_generation}, 0, 'Log', NULL, NULL, 0)`
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
        meta.active_schema_generation === expected.generation &&
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
      const after = state.cursor_sequence ?? 0
      const metadata = yield* logMetadata({ after, limit: batchSize }).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      const limit = yield* boundedCount(metadata, batchBytes)
      const rows = yield* logBatch({ after, limit }).pipe(Effect.mapError(StorageUnavailable.make))
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        for (const row of rows) {
          const entry = yield* currentOrLegacyEntry(row, source, definition)
          for (const change of entry.changes) {
            const migrated = yield* migrateEntityChange(options.evolution, entry.sourceSchema, change)
            yield* registerLineage(change.entity.model, migrated)
          }
          if (row.source_schema_version === null || row.source_schema_hash === null) {
            yield* sql`UPDATE effect_local_server_log SET entry_json = ${yield* Codec.stringify(entry)},
                source_schema_version = ${entry.sourceSchema.version}, source_schema_hash = ${entry.sourceSchema.hash}
                WHERE space_id = ${options.spaceId} AND server_sequence = ${row.server_sequence}`
          }
        }
        if (rows.length === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'Entities', cursor_sequence = NULL,
              cursor_model = NULL, cursor_key = NULL
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        } else {
          yield* sql`UPDATE effect_local_client_evolution SET cursor_sequence = ${rows[rows.length - 1].server_sequence}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
        return undefined
      }))
    } else if (state.phase === "Entities") {
      let metadata: ReadonlyArray<typeof EntityBytesRow.Type>
      if (state.cursor_model === null) {
        metadata = yield* initialEntityMetadata({
          generation: state.source_generation,
          limit: batchSize
        }).pipe(Effect.mapError(StorageUnavailable.make))
      } else {
        metadata = yield* continuingEntityMetadata({
          generation: state.source_generation,
          model: state.cursor_model,
          key: state.cursor_key!,
          limit: batchSize
        }).pipe(Effect.mapError(StorageUnavailable.make))
      }
      const limit = yield* boundedCount(metadata, batchBytes)
      let rows: ReadonlyArray<typeof EntityBatchRow.Type>
      if (state.cursor_model === null) {
        rows = yield* initialEntityBatch({ generation: state.source_generation, limit }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
      } else {
        rows = yield* continuingEntityBatch({
          generation: state.source_generation,
          model: state.cursor_model,
          key: state.cursor_key!,
          limit
        }).pipe(Effect.mapError(StorageUnavailable.make))
      }
      yield* withTransaction(Effect.gen(function*() {
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
          const keyJson = yield* Codec.stringify(migrated.key)
          const valueJson = yield* Codec.stringify(migrated.value)
          yield* sql`INSERT INTO effect_local_client_canonical_entities_data
              (space_id, schema_generation, model, model_version, entity_key, value_json)
              VALUES (${options.spaceId}, ${state.generation}, ${row.model}, ${migrated.modelVersion},
                ${keyJson}, ${valueJson})`
          yield* sql`INSERT INTO effect_local_client_visible_entities_data
              (space_id, schema_generation, projection_generation, model, model_version, entity_key, value_json)
              VALUES (${options.spaceId}, ${state.generation}, ${state.target_projection_generation},
                ${row.model}, ${migrated.modelVersion}, ${keyJson}, ${valueJson})`
        }
        if (rows.length === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'Receipts', cursor_model = NULL,
              cursor_key = NULL, cursor_sequence = 0
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        } else {
          const last = rows[rows.length - 1]
          yield* sql`UPDATE effect_local_client_evolution SET cursor_model = ${last.model}, cursor_key = ${last.entity_key}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
        return undefined
      }))
    } else if (state.phase === "Receipts") {
      const request = {
        generation: state.source_generation,
        after: state.cursor_sequence ?? 0,
        limit: batchSize
      }
      const metadata = yield* receiptMetadata(request).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      const limit = yield* boundedCount(metadata, batchBytes)
      const rows = yield* receiptBatch({ ...request, limit }).pipe(Effect.mapError(StorageUnavailable.make))
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        for (const row of rows) {
          const decoded = yield* currentOrLegacyReceipt(row, source)
          const receipt = yield* migrateReceipt(decoded, options.evolution)
          const protocolMetadata = protocolReceiptMetadata(receipt)
          yield* sql`INSERT INTO effect_local_client_receipts_data
              (space_id, schema_generation, membership_incarnation, mutation_id, local_sequence, receipt_json,
                source_schema_version, source_schema_hash, mutation_version, mutation_name, rejection_origin)
              VALUES (${options.spaceId}, ${state.generation}, ${receipt.membershipIncarnation},
                ${receipt.mutationId}, ${receipt.localSequence},
                ${yield* Codec.stringify(receipt)}, ${receipt.sourceSchema.version}, ${receipt.sourceSchema.hash},
                ${protocolMetadata.mutationVersion}, ${protocolMetadata.mutationName},
                ${protocolMetadata.rejectionOrigin})`
        }
        if (rows.length === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'Pending', cursor_sequence = 0
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        } else {
          yield* sql`UPDATE effect_local_client_evolution SET cursor_sequence = ${rows[rows.length - 1].local_sequence}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
        return undefined
      }))
    } else if (state.phase === "Pending") {
      const request = {
        generation: state.source_generation,
        after: state.cursor_sequence ?? 0,
        limit: batchSize
      }
      const metadata = yield* pendingMetadata(request).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      const limit = yield* boundedCount(metadata, batchBytes)
      const rows = yield* pendingBatch({ ...request, limit }).pipe(Effect.mapError(StorageUnavailable.make))
      yield* Effect.gen(function*() {
        yield* validateBatch(state)
        for (const row of rows) {
          const envelope = yield* pendingEnvelope(row, options, source, definition)
          const decodedChanges = yield* decodeJson(
            Schema.Array(Protocol.EntityChange),
            row.changes_json
          ).pipe(Effect.result)
          let historicalChanges: ReadonlyArray<Protocol.EntityChange>
          if (Result.isSuccess(decodedChanges)) {
            historicalChanges = decodedChanges.success
          } else {
            historicalChanges = yield* decodeJson(Schema.Array(LegacyEntityChange), row.changes_json).pipe(
              Effect.flatMap(Effect.forEach((change: typeof LegacyEntityChange.Type): Effect.Effect<
                Protocol.EntityChange,
                ReplicaError.StorageCorrupt
              > => {
                const model = definition.modelByName.get(change.entity.model)
                if (model === undefined) {
                  return Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: `Pending mutation references unknown model ${change.entity.model}`
                    })
                  )
                }
                if (change._tag === "Delete") {
                  return Effect.succeed({
                    _tag: "Delete" as const,
                    entity: { ...change.entity, modelVersion: model.version }
                  })
                }
                return Effect.succeed({
                  _tag: "Upsert" as const,
                  entity: { ...change.entity, modelVersion: model.version },
                  value: change.value
                })
              }))
            )
          }
          const migratedHistoricalChanges: Array<Protocol.EntityChange> = []
          for (const change of historicalChanges) {
            const migrated = yield* migrateEntityChange(options.evolution, source, change)
            yield* registerLineage(change.entity.model, migrated)
            const entity = {
              model: change.entity.model,
              modelVersion: migrated.modelVersion,
              key: migrated.key
            }
            if (change._tag === "Delete") {
              migratedHistoricalChanges.push(Protocol.Delete.make({ entity }))
            } else {
              if (migrated.value === undefined) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Migrated pending upsert for ${change.entity.model} has no value`
                })
              }
              migratedHistoricalChanges.push(Protocol.Upsert.make({ entity, value: migrated.value }))
            }
          }
          if (row.submission_state === "AwaitingReceipt" || row.receipt_count > 0) {
            const sourceMutation = definition.mutationByName.get(envelope.name)
            if (sourceMutation === undefined) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Pending mutation references unknown mutation ${envelope.name}`
              })
            }
            const optimisticResult = yield* Evolution.migrateMutationSuccess({
              evolution: options.evolution,
              source,
              mutation: envelope.name,
              mutationVersion: sourceMutation.version,
              value: yield* decodeJson(Schema.Json, row.optimistic_result_json)
            })
            yield* sql`INSERT INTO effect_local_client_pending_data
                (space_id, schema_generation, membership_incarnation, mutation_id, local_sequence, basis, name,
                  payload_json, digest, digest_version, source_schema_version, source_schema_hash, mutation_version,
                  optimistic_result_json, changes_json, submission_state, attempt_count)
                VALUES (${options.spaceId}, ${state.generation}, ${envelope.membershipIncarnation},
                  ${envelope.mutationId}, ${envelope.localSequence}, ${envelope.basis},
                  ${envelope.name}, ${yield* Codec.stringify(envelope.payload)}, ${envelope.digest},
                  ${envelope.digestVersion}, ${envelope.sourceSchema.version}, ${envelope.sourceSchema.hash},
                  ${envelope.mutationVersion}, ${yield* Codec.stringify(optimisticResult.value)},
                  ${yield* Codec.stringify(migratedHistoricalChanges)}, ${row.submission_state}, ${row.attempt_count})`
            continue
          }
          const changes: Array<Protocol.EntityChange> = []
          const executed = yield* runtime.executeEnvelope(
            envelope,
            SqlTransaction.local({
              sql,
              table: "visible",
              spaceId: options.spaceId,
              schemaGeneration: state.generation,
              projectionGeneration: state.target_projection_generation,
              changes
            }),
            changes
          ).pipe(
            Effect.flatMap(
              (result) => {
                if (Result.isSuccess(result)) return Effect.succeed(result.success)
                return Effect.fail(
                  new TerminalRejection.TerminalRejection({
                    origin: "Mutation",
                    rejection: result.failure
                  })
                )
              }
            ),
            withTransaction,
            Effect.map(Result.succeed),
            Effect.catchTag("TerminalRejection", ({ rejection }) => Effect.succeed(Result.fail(rejection)))
          )
          if (Result.isFailure(executed)) {
            yield* sql`INSERT INTO effect_local_client_quarantine
                (space_id, membership_incarnation, mutation_id, local_sequence, basis, name, payload_json,
                  digest, digest_version, source_schema_version, source_schema_hash, mutation_version,
                  rejection_json, target_schema_version, target_schema_hash)
                VALUES (${options.spaceId}, ${envelope.membershipIncarnation}, ${envelope.mutationId},
                  ${envelope.localSequence}, ${envelope.basis}, ${envelope.name},
                  ${yield* Codec.stringify(envelope.payload)}, ${envelope.digest}, ${envelope.digestVersion},
                  ${envelope.sourceSchema.version}, ${envelope.sourceSchema.hash}, ${envelope.mutationVersion},
                  ${yield* Codec.stringify(executed.failure)}, ${options.definition.schemaIdentity.version},
                  ${options.definition.schemaIdentity.hash})
                ON CONFLICT (space_id, mutation_id) DO NOTHING`
            continue
          }
          yield* sql`INSERT INTO effect_local_client_pending_data
              (space_id, schema_generation, membership_incarnation, mutation_id, local_sequence, basis, name,
                payload_json, digest, digest_version, source_schema_version, source_schema_hash, mutation_version,
                optimistic_result_json, changes_json, submission_state, attempt_count)
              VALUES (${options.spaceId}, ${state.generation}, ${envelope.membershipIncarnation},
                ${envelope.mutationId}, ${envelope.localSequence}, ${envelope.basis},
                ${envelope.name}, ${yield* Codec.stringify(envelope.payload)}, ${envelope.digest},
                ${envelope.digestVersion}, ${envelope.sourceSchema.version}, ${envelope.sourceSchema.hash},
                ${envelope.mutationVersion}, ${yield* Codec.stringify(executed.success.result)},
                ${yield* Codec.stringify(changes)}, ${row.submission_state}, ${row.attempt_count})`
        }
        if (rows.length === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'Retractions', cursor_model = NULL,
              cursor_key = NULL, cursor_sequence = NULL
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        } else {
          yield* sql`UPDATE effect_local_client_evolution SET cursor_sequence = ${rows[rows.length - 1].local_sequence}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
        return undefined
      }).pipe(withTransaction)
    } else if (state.phase === "Retractions") {
      const request = {
        generation: state.source_generation,
        model: state.cursor_model,
        key: state.cursor_key,
        limit: batchSize
      }
      let metadata: ReadonlyArray<typeof EntityBytesRow.Type>
      if (state.cursor_model === null || state.cursor_key === null) {
        metadata = yield* initialRetractionMetadata({
          generation: request.generation,
          limit: request.limit
        }).pipe(Effect.mapError(StorageUnavailable.make))
      } else {
        metadata = yield* continuingRetractionMetadata({
          generation: request.generation,
          model: state.cursor_model,
          key: state.cursor_key,
          limit: request.limit
        }).pipe(Effect.mapError(StorageUnavailable.make))
      }
      const limit = yield* boundedCount(metadata, batchBytes)
      let rows: ReadonlyArray<typeof RetractionBatchRow.Type>
      if (state.cursor_model === null || state.cursor_key === null) {
        rows = yield* initialRetractionBatch({ generation: request.generation, limit }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
      } else {
        rows = yield* continuingRetractionBatch({
          generation: request.generation,
          model: state.cursor_model,
          key: state.cursor_key,
          limit
        }).pipe(Effect.mapError(StorageUnavailable.make))
      }
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        for (const row of rows) {
          const migrated = yield* Evolution.migrateModel({
            evolution: options.evolution,
            source,
            model: row.model,
            modelVersion: row.model_version,
            key: yield* decodeJson(Schema.Json, row.entity_key)
          })
          yield* registerLineage(row.model, migrated)
          const key = yield* Codec.stringify(migrated.key)
          yield* sql`INSERT INTO effect_local_client_retractions
              (space_id, generation, model, model_version, entity_key)
              VALUES (${options.spaceId}, ${state.generation}, ${row.model}, ${migrated.modelVersion}, ${key})
              ON CONFLICT (space_id, generation, model, entity_key) DO UPDATE SET
                model_version = excluded.model_version`
          yield* sql`DELETE FROM effect_local_client_visible_entities_data
              WHERE space_id = ${options.spaceId} AND schema_generation = ${state.generation}
                AND model = ${row.model} AND entity_key = ${key}`
        }
        if (rows.length === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'Flip', cursor_model = NULL,
              cursor_key = NULL WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        } else {
          const last = rows[rows.length - 1]
          yield* sql`UPDATE effect_local_client_evolution SET cursor_model = ${last.model},
              cursor_key = ${last.entity_key}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "Flip") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_client_scoped_bootstrap_entries
            WHERE space_id = ${options.spaceId}`
        yield* sql`DELETE FROM effect_local_client_scoped_bootstrap WHERE space_id = ${options.spaceId}`
        yield* sql`UPDATE effect_local_client_spaces SET active_schema_generation = ${state.generation},
            active_projection_generation = ${state.target_projection_generation},
            projection_schema_generation = ${state.generation},
            projection_replay_generation = NULL, projection_replay_cursor = NULL,
            visible_revision = visible_revision + 1, replication_view_id = NULL,
            replication_view_revision = 0, installed_snapshot_id = NULL,
            installed_snapshot_sequence = 0, installed_snapshot_terminal_sequence = 0
            WHERE space_id = ${options.spaceId} AND schema_generation = ${state.generation}
              AND active_schema_generation = ${state.source_generation}`
        yield* sql`UPDATE effect_local_client_evolution SET phase = 'CleanupCanonical'
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
      }))
    } else if (state.phase === "CleanupCanonical") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_client_canonical_entities_data WHERE rowid IN (
            SELECT rowid FROM effect_local_client_canonical_entities_data
            WHERE space_id = ${options.spaceId} AND schema_generation = ${state.source_generation}
            ORDER BY model, entity_key LIMIT ${batchSize})`
        const remaining = yield* countCanonicalGeneration(state.source_generation).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'CleanupVisible'
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "CleanupVisible") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_client_visible_entities_data WHERE rowid IN (
            SELECT rowid FROM effect_local_client_visible_entities_data
            WHERE space_id = ${options.spaceId} AND schema_generation = ${state.source_generation}
            ORDER BY model, entity_key LIMIT ${batchSize})`
        const remaining = yield* countVisibleGeneration(state.source_generation).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'CleanupRetractions'
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "CleanupRetractions") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_client_retractions WHERE rowid IN (
            SELECT rowid FROM effect_local_client_retractions
            WHERE space_id = ${options.spaceId} AND generation = ${state.source_generation}
            ORDER BY model, entity_key LIMIT ${batchSize})`
        const remaining = yield* countRetractionGeneration(state.source_generation).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'CleanupReceipts'
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "CleanupReceipts") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_client_receipts_data WHERE rowid IN (
            SELECT rowid FROM effect_local_client_receipts_data
            WHERE space_id = ${options.spaceId} AND schema_generation = ${state.source_generation}
            ORDER BY local_sequence LIMIT ${batchSize})`
        const remaining = yield* countReceiptGeneration(state.source_generation).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'CleanupPending'
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "CleanupPending") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_client_pending_data WHERE rowid IN (
            SELECT rowid FROM effect_local_client_pending_data
            WHERE space_id = ${options.spaceId} AND schema_generation = ${state.source_generation}
            ORDER BY local_sequence LIMIT ${batchSize})`
        const remaining = yield* countPendingGeneration(state.source_generation).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_client_evolution SET phase = 'Finalize'
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else {
      yield* Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`UPDATE effect_local_client_spaces SET definition_hash = ${options.definition.hash},
            schema_version = ${options.definition.schemaIdentity.version},
            schema_hash = ${options.definition.schemaIdentity.hash}, target_schema_version = NULL,
            target_schema_hash = NULL, migration_hash = NULL
            WHERE space_id = ${options.spaceId} AND schema_generation = ${state.generation}
              AND active_schema_generation = ${state.generation}`
        yield* sql`DELETE FROM effect_local_client_evolution
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
      }).pipe(withTransaction)
    }
    if (options.afterBatch !== undefined) yield* options.afterBatch
  }
}, Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

const ServerMetaEvolutionRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: NullableSchemaVersion,
  schema_hash: NullableSchemaHash,
  schema_generation: NonNegativeInt,
  active_schema_generation: NonNegativeInt,
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
  source_generation: NonNegativeInt,
  target_entity_count: NonNegativeInt,
  target_entity_bytes: NonNegativeInt,
  phase: Schema.Literals([
    "Log",
    "Entities",
    "Receipts",
    "Flip",
    "CleanupScopedSnapshotEntries",
    "CleanupScopedSnapshots",
    "CleanupReplicationPages",
    "CleanupReplicationViewEntities",
    "CleanupReplicationViews",
    "CleanupEntities",
    "Finalize"
  ]),
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
  readonly batchBytes?: number | undefined
  readonly afterBatch?: Effect.Effect<void> | undefined
}

export const server = Effect.fn("SchemaEvolution.server")(function*(options: ServerOptions) {
  yield* Effect.annotateCurrentSpan("space.id", options.spaceId)
  const sql = yield* SqlClient.SqlClient
  const withTransaction = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
    sql.withTransaction(effect)
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
  const batchBytes = options.batchBytes ?? Protocol.maximumBatchBytes
  if (!Number.isSafeInteger(batchBytes) || batchBytes < 1) {
    return yield* new ReplicaError.InvalidConfiguration({
      option: "schemaEvolutionBatchBytes",
      message: "Schema evolution batch bytes must be a positive safe integer"
    })
  }

  const readMeta = SqlSchema.findOne({
    Request: Schema.Void,
    Result: ServerMetaEvolutionRow,
    execute: () =>
      sql`SELECT definition_hash, schema_version, schema_hash, schema_generation, active_schema_generation,
        target_schema_version, target_schema_hash, migration_hash FROM effect_local_server_spaces
        WHERE space_id = ${options.spaceId}`
  })
  const readProgress = SqlSchema.findOneOption({
    Request: Schema.Void,
    Result: ServerProgressRow,
    execute: () =>
      sql`SELECT source_schema_version, source_schema_hash, target_schema_version,
        target_schema_hash, migration_hash, generation, source_generation, target_entity_count,
        target_entity_bytes, phase, cursor_model, cursor_key, cursor_sequence
        FROM effect_local_server_evolution WHERE space_id = ${options.spaceId}`
  })
  const countEntityGeneration = SqlSchema.findOne({
    Request: NonNegativeInt,
    Result: CountRow,
    execute: (generation) =>
      sql`SELECT COUNT(*) AS count FROM effect_local_server_entities_data
        WHERE space_id = ${options.spaceId} AND generation = ${generation}`
  })
  const countScopedSnapshotEntries = SqlSchema.findOne({
    Request: Schema.Void,
    Result: CountRow,
    execute: () =>
      sql`SELECT EXISTS(SELECT 1 FROM effect_local_server_scoped_snapshot_entries
        WHERE snapshot_id IN (
          SELECT snapshot_id FROM effect_local_server_scoped_snapshots
          WHERE space_id = ${options.spaceId}
        )) AS count`
  })
  const countScopedSnapshots = SqlSchema.findOne({
    Request: Schema.Void,
    Result: CountRow,
    execute: () =>
      sql`SELECT EXISTS(SELECT 1 FROM effect_local_server_scoped_snapshots
        WHERE space_id = ${options.spaceId}) AS count`
  })
  const countReplicationPages = SqlSchema.findOne({
    Request: Schema.Void,
    Result: CountRow,
    execute: () =>
      sql`SELECT EXISTS(SELECT 1 FROM effect_local_server_replication_pages AS page
        WHERE page.space_id = ${options.spaceId}) AS count`
  })
  const countReplicationViewEntities = SqlSchema.findOne({
    Request: Schema.Void,
    Result: CountRow,
    execute: () =>
      sql`SELECT EXISTS(SELECT 1 FROM effect_local_server_replication_view_entities AS entity
        WHERE entity.space_id = ${options.spaceId}) AS count`
  })
  const countReplicationViews = SqlSchema.findOne({
    Request: Schema.Void,
    Result: CountRow,
    execute: () =>
      sql`SELECT EXISTS(SELECT 1 FROM effect_local_server_replication_views
        WHERE space_id = ${options.spaceId}) AS count`
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
          legacy_schema_version = COALESCE(legacy_schema_version, ${sourceVersion}),
          legacy_schema_hash = COALESCE(legacy_schema_hash, ${sourceHash}),
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
  const initialEntityMetadata = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, limit: Schema.Number }),
    Result: EntityBytesRow,
    execute: ({ generation, limit }) =>
      sql`SELECT model, entity_key,
          length(CAST(entity_key AS BLOB)) + length(CAST(value_json AS BLOB)) AS row_bytes
        FROM effect_local_server_entities_data WHERE space_id = ${options.spaceId}
          AND generation = ${generation} ORDER BY model, entity_key LIMIT ${limit}`
  })
  const continuingEntityMetadata = SqlSchema.findAll({
    Request: Schema.Struct({
      generation: NonNegativeInt,
      model: Schema.String,
      key: Schema.String,
      limit: Schema.Number
    }),
    Result: EntityBytesRow,
    execute: ({ generation, model, key, limit }) =>
      sql`SELECT model, entity_key,
          length(CAST(entity_key AS BLOB)) + length(CAST(value_json AS BLOB)) AS row_bytes
        FROM effect_local_server_entities_data WHERE space_id = ${options.spaceId}
          AND generation = ${generation} AND (model > ${model} OR (model = ${model} AND entity_key > ${key}))
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const initialEntityBatch = SqlSchema.findAll({
    Request: Schema.Struct({ generation: NonNegativeInt, limit: Schema.Number }),
    Result: EntityBatchRow,
    execute: ({ generation, limit }) =>
      sql`SELECT model, model_version, entity_key, value_json
        FROM effect_local_server_entities_data WHERE space_id = ${options.spaceId}
          AND generation = ${generation} ORDER BY model, entity_key LIMIT ${limit}`
  })
  const continuingEntityBatch = SqlSchema.findAll({
    Request: Schema.Struct({
      generation: NonNegativeInt,
      model: Schema.String,
      key: Schema.String,
      limit: Schema.Number
    }),
    Result: EntityBatchRow,
    execute: ({ generation, model, key, limit }) =>
      sql`SELECT model, model_version, entity_key, value_json
        FROM effect_local_server_entities_data WHERE space_id = ${options.spaceId}
          AND generation = ${generation} AND (model > ${model} OR (model = ${model} AND entity_key > ${key}))
        ORDER BY model, entity_key LIMIT ${limit}`
  })
  const initialReceiptMetadata = SqlSchema.findAll({
    Request: Schema.Number,
    Result: KeyBytesRow,
    execute: (limit) =>
      sql`SELECT mutation_id, length(CAST(receipt_json AS BLOB)) AS row_bytes
        FROM effect_local_server_receipts WHERE space_id = ${options.spaceId}
        ORDER BY mutation_id LIMIT ${limit}`
  })
  const continuingReceiptMetadata = SqlSchema.findAll({
    Request: Schema.Struct({ after: Schema.String, limit: Schema.Number }),
    Result: KeyBytesRow,
    execute: ({ after, limit }) =>
      sql`SELECT mutation_id, length(CAST(receipt_json AS BLOB)) AS row_bytes
        FROM effect_local_server_receipts WHERE space_id = ${options.spaceId} AND mutation_id > ${after}
        ORDER BY mutation_id LIMIT ${limit}`
  })
  const initialReceiptBatch = SqlSchema.findAll({
    Request: Schema.Number,
    Result: ServerReceiptBatchRow,
    execute: (limit) =>
      sql`SELECT mutation_id, local_sequence, receipt_json,
        source_schema_version, source_schema_hash, mutation_version, mutation_name, rejection_origin
        FROM effect_local_server_receipts WHERE space_id = ${options.spaceId}
        ORDER BY mutation_id LIMIT ${limit}`
  })
  const continuingReceiptBatch = SqlSchema.findAll({
    Request: Schema.Struct({ after: Schema.String, limit: Schema.Number }),
    Result: ServerReceiptBatchRow,
    execute: ({ after, limit }) =>
      sql`SELECT mutation_id, local_sequence, receipt_json,
        source_schema_version, source_schema_hash, mutation_version, mutation_name, rejection_origin
        FROM effect_local_server_receipts WHERE space_id = ${options.spaceId} AND mutation_id > ${after}
        ORDER BY mutation_id LIMIT ${limit}`
  })
  const logMetadata = SqlSchema.findAll({
    Request: Schema.Struct({ after: NonNegativeInt, limit: Schema.Number }),
    Result: SequenceBytesRow,
    execute: ({ after, limit }) =>
      sql`SELECT server_sequence, length(CAST(entry_json AS BLOB)) AS row_bytes
        FROM effect_local_authoritative_log WHERE space_id = ${options.spaceId}
          AND server_sequence > ${after} ORDER BY server_sequence LIMIT ${limit}`
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

  const registerLineage = Effect.fnUntraced(function*(model: string, migrated: Evolution.MigratedModel) {
    const aliases = yield* Effect.forEach(
      migrated.aliases,
      (alias) => Codec.stringify(alias.key).pipe(Effect.map((key) => ({ ...alias, key })))
    )
    let sourceAliases = aliases
    if (aliases.length !== 1) sourceAliases = aliases.slice(0, -1)
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
    return undefined
  })

  const validateBatch = Effect.fnUntraced(function*(state: typeof ServerProgressRow.Type) {
    const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    const progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    let expectedActiveGeneration: number = state.generation
    if (
      state.phase === "Flip" || state.phase === "Log" || state.phase === "Entities" ||
      state.phase === "Receipts"
    ) {
      expectedActiveGeneration = state.source_generation
    }
    if (
      meta.schema_generation !== state.generation || meta.schema_version !== state.source_schema_version ||
      meta.active_schema_generation !== expectedActiveGeneration ||
      meta.schema_hash !== state.source_schema_hash || meta.target_schema_version !== state.target_schema_version ||
      meta.target_schema_hash !== state.target_schema_hash || meta.migration_hash !== state.migration_hash ||
      Option.isNone(progress) || !sameProgress(progress.value, state) ||
      progress.value.target_entity_count !== state.target_entity_count ||
      progress.value.target_entity_bytes !== state.target_entity_bytes
    ) {
      return yield* new ReplicaError.SchemaGenerationConflict({
        expected: state.generation,
        actual: meta.schema_generation
      })
    }
    return undefined
  })

  let progress = yield* readProgress(undefined).pipe(Effect.mapError(StorageUnavailable.make))
  if (Option.isNone(progress)) {
    const meta = yield* readMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    const source = yield* resolveInitialSource(meta, options.evolution)
    if (!source.legacy && sameIdentity(source.identity, options.definition.schemaIdentity)) {
      if (meta.definition_hash === options.definition.hash) return meta.schema_generation
      return yield* withTransaction(Effect.gen(function*() {
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
    yield* withTransaction(Effect.gen(function*() {
      const promoted = yield* beginPromotion({
        expectedGeneration: meta.schema_generation,
        generation,
        sourceVersion: source.identity.version,
        sourceHash: source.identity.hash
      }).pipe(Effect.mapError(StorageUnavailable.make))
      if (Option.isNone(promoted)) return
      yield* sql`DELETE FROM effect_local_server_entities_data
          WHERE space_id = ${options.spaceId} AND generation = ${generation}`
      yield* sql`INSERT INTO effect_local_server_evolution
          (space_id, source_schema_version, source_schema_hash, target_schema_version,
            target_schema_hash, migration_hash, generation, source_generation,
            target_entity_count, target_entity_bytes, phase, cursor_model, cursor_key, cursor_sequence)
          VALUES (${options.spaceId}, ${source.identity.version}, ${source.identity.hash},
            ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash},
            ${options.evolution.migrationHash}, ${generation}, ${meta.active_schema_generation},
            0, 0, 'Log', NULL, NULL, 0)`
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
        meta.active_schema_generation === expected.generation &&
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
      const after = state.cursor_sequence ?? 0
      const metadata = yield* logMetadata({ after, limit: batchSize }).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      const limit = yield* boundedCount(metadata, batchBytes)
      const rows = yield* logBatch({ after, limit }).pipe(Effect.mapError(StorageUnavailable.make))
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        for (const row of rows) {
          const entry = yield* currentOrLegacyEntry(row, source, definition)
          for (const change of entry.changes) {
            const migrated = yield* migrateEntityChange(options.evolution, entry.sourceSchema, change)
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
          yield* sql`UPDATE effect_local_server_evolution SET cursor_sequence = ${rows[rows.length - 1].server_sequence}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "Entities") {
      let targetEntityCount = 0
      let targetEntityBytes = 0
      let metadata: ReadonlyArray<typeof EntityBytesRow.Type>
      if (state.cursor_model === null) {
        metadata = yield* initialEntityMetadata({
          generation: state.source_generation,
          limit: batchSize
        }).pipe(Effect.mapError(StorageUnavailable.make))
      } else {
        metadata = yield* continuingEntityMetadata({
          generation: state.source_generation,
          model: state.cursor_model,
          key: state.cursor_key!,
          limit: batchSize
        }).pipe(Effect.mapError(StorageUnavailable.make))
      }
      const limit = yield* boundedCount(metadata, batchBytes)
      let rows: ReadonlyArray<typeof EntityBatchRow.Type>
      if (state.cursor_model === null) {
        rows = yield* initialEntityBatch({ generation: state.source_generation, limit }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
      } else {
        rows = yield* continuingEntityBatch({
          generation: state.source_generation,
          model: state.cursor_model,
          key: state.cursor_key!,
          limit
        }).pipe(Effect.mapError(StorageUnavailable.make))
      }
      yield* withTransaction(Effect.gen(function*() {
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
          const key = migrated.key
          const value = migrated.value
          const keyJson = yield* Codec.stringify(key)
          const valueJson = yield* Codec.stringify(value)
          const entityBytes = yield* Protocol.encodedBytesEffect({
            model: row.model,
            modelVersion: migrated.modelVersion,
            key,
            value
          })
          yield* sql`INSERT INTO effect_local_server_entities_data
              (space_id, generation, model, model_version, entity_key, value_json, entity_bytes)
              VALUES (${options.spaceId}, ${state.generation}, ${row.model}, ${migrated.modelVersion},
                ${keyJson}, ${valueJson}, ${entityBytes})`
          targetEntityCount += 1
          targetEntityBytes += entityBytes
        }
        if (targetEntityCount > 0) {
          yield* sql`UPDATE effect_local_server_evolution SET
              target_entity_count = target_entity_count + ${targetEntityCount},
              target_entity_bytes = target_entity_bytes + ${targetEntityBytes}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
        if (rows.length === 0) {
          yield* sql`UPDATE effect_local_server_evolution SET phase = 'Receipts', cursor_model = NULL,
              cursor_key = NULL, cursor_sequence = 0 WHERE space_id = ${options.spaceId}
              AND generation = ${state.generation}`
        } else {
          const last = rows[rows.length - 1]
          yield* sql`UPDATE effect_local_server_evolution SET cursor_model = ${last.model}, cursor_key = ${last.entity_key}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
        return undefined
      }))
    } else if (state.phase === "Receipts") {
      let metadataQuery = initialReceiptMetadata(batchSize)
      if (state.cursor_key !== null) {
        metadataQuery = continuingReceiptMetadata({ after: state.cursor_key, limit: batchSize })
      }
      const metadata = yield* metadataQuery.pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      const limit = yield* boundedCount(metadata, batchBytes)
      let receiptQuery = initialReceiptBatch(limit)
      if (state.cursor_key !== null) receiptQuery = continuingReceiptBatch({ after: state.cursor_key, limit })
      const rows = yield* receiptQuery.pipe(Effect.mapError(StorageUnavailable.make))
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        for (const row of rows) {
          const receipt = yield* currentOrLegacyReceipt(row, source)
          const protocolMetadata = protocolReceiptMetadata(receipt)
          yield* sql`UPDATE effect_local_server_receipts SET receipt_json = ${yield* Codec.stringify(receipt)},
              source_schema_version = ${receipt.sourceSchema.version}, source_schema_hash = ${receipt.sourceSchema.hash},
              mutation_version = ${protocolMetadata.mutationVersion},
              mutation_name = ${protocolMetadata.mutationName},
              rejection_origin = ${protocolMetadata.rejectionOrigin}
              WHERE space_id = ${options.spaceId} AND mutation_id = ${row.mutation_id}`
        }
        if (rows.length === 0) {
          yield* sql`UPDATE effect_local_server_evolution SET phase = 'Flip', cursor_key = NULL,
              cursor_sequence = NULL
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        } else {
          yield* sql`UPDATE effect_local_server_evolution SET cursor_key = ${rows[rows.length - 1].mutation_id}
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "Flip") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`UPDATE effect_local_server_spaces SET
            active_schema_generation = ${state.generation},
            entity_count = ${state.target_entity_count}, entity_bytes = ${state.target_entity_bytes}
            WHERE space_id = ${options.spaceId} AND schema_generation = ${state.generation}
              AND active_schema_generation = ${state.source_generation}`
        yield* sql`UPDATE effect_local_server_evolution SET phase = 'CleanupScopedSnapshotEntries'
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
      }))
    } else if (state.phase === "CleanupScopedSnapshotEntries") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_server_scoped_snapshot_entries WHERE rowid IN (
            SELECT entry.rowid FROM effect_local_server_scoped_snapshot_entries AS entry
            INNER JOIN effect_local_server_scoped_snapshots AS snapshot
              ON snapshot.snapshot_id = entry.snapshot_id
            WHERE snapshot.space_id = ${options.spaceId}
            ORDER BY entry.snapshot_id, entry.ordinal LIMIT ${batchSize})`
        const remaining = yield* countScopedSnapshotEntries(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_server_evolution SET phase = 'CleanupScopedSnapshots'
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "CleanupScopedSnapshots") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_server_scoped_snapshots WHERE rowid IN (
            SELECT rowid FROM effect_local_server_scoped_snapshots
            WHERE space_id = ${options.spaceId}
            ORDER BY snapshot_id LIMIT ${batchSize})`
        const remaining = yield* countScopedSnapshots(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_server_evolution SET phase = 'CleanupReplicationPages'
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "CleanupReplicationPages") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_server_replication_pages WHERE rowid IN (
            SELECT page.rowid FROM effect_local_server_replication_pages AS page
            INNER JOIN effect_local_server_replication_views AS view
              ON view.space_id = page.space_id AND view.client_id = page.client_id
            WHERE view.space_id = ${options.spaceId}
            ORDER BY page.client_id LIMIT ${batchSize})`
        const remaining = yield* countReplicationPages(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_server_evolution SET phase = 'CleanupReplicationViewEntities'
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "CleanupReplicationViewEntities") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_server_replication_view_entities WHERE rowid IN (
            SELECT entity.rowid FROM effect_local_server_replication_view_entities AS entity
            INNER JOIN effect_local_server_replication_views AS view
              ON view.space_id = entity.space_id AND view.client_id = entity.client_id
            WHERE view.space_id = ${options.spaceId}
            ORDER BY entity.client_id, entity.model, entity.entity_key LIMIT ${batchSize})`
        const remaining = yield* countReplicationViewEntities(undefined).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_server_evolution SET phase = 'CleanupReplicationViews'
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "CleanupReplicationViews") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_server_replication_views WHERE rowid IN (
            SELECT rowid FROM effect_local_server_replication_views
            WHERE space_id = ${options.spaceId}
            ORDER BY client_id LIMIT ${batchSize})`
        const remaining = yield* countReplicationViews(undefined).pipe(Effect.mapError(StorageUnavailable.make))
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_server_evolution SET phase = 'CleanupEntities'
              WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else if (state.phase === "CleanupEntities") {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`DELETE FROM effect_local_server_entities_data WHERE rowid IN (
            SELECT rowid FROM effect_local_server_entities_data
            WHERE space_id = ${options.spaceId} AND generation = ${state.source_generation}
            ORDER BY model, entity_key LIMIT ${batchSize})`
        const remaining = yield* countEntityGeneration(state.source_generation).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (remaining.count === 0) {
          yield* sql`UPDATE effect_local_server_evolution SET phase = 'Finalize'
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
        }
      }))
    } else {
      yield* withTransaction(Effect.gen(function*() {
        yield* validateBatch(state)
        yield* sql`UPDATE effect_local_server_spaces SET definition_hash = ${options.definition.hash},
            schema_version = ${options.definition.schemaIdentity.version},
            schema_hash = ${options.definition.schemaIdentity.hash}, target_schema_version = NULL,
            target_schema_hash = NULL, migration_hash = NULL
            WHERE space_id = ${options.spaceId} AND schema_generation = ${state.generation}
              AND active_schema_generation = ${state.generation}`
        yield* sql`DELETE FROM effect_local_server_evolution
            WHERE space_id = ${options.spaceId} AND generation = ${state.generation}`
      }))
    }
    if (options.afterBatch !== undefined) yield* options.afterBatch
  }
}, Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
