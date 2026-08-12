import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as SchemaDescriptor from "@lucas-barake/effect-local/SchemaDescriptor"
import type * as SecondaryIndex from "@lucas-barake/effect-local/SecondaryIndex"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import type * as Statement from "effect/unstable/sql/Statement"
import * as Codec from "./internal/codec.js"
import { affinitySql, encodeComponent } from "./internal/indexComponents.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

type SqlValue = string | number

export interface Address {
  readonly spaceId: Identity.SpaceId
  readonly schemaGeneration: number
  readonly projectionGeneration: number
}

interface Descriptor {
  readonly model: Model.Any
  readonly indexName: string
  readonly index: SecondaryIndex.Any
  readonly hash: string
  readonly tableName: string
  readonly scanIndexName: string
  readonly tableDdl: string
  readonly scanIndexDdl: string
  readonly tableSchema: string
  readonly scanIndexSchema: string
  readonly tableChecksum: string
  readonly scanIndexChecksum: string
}

export interface Point {
  readonly spaceId: Identity.SpaceId
  readonly descriptor: string
  readonly model: string
  readonly index: string
  readonly partition: ReadonlyArray<SqlValue>
  readonly sort: ReadonlyArray<SqlValue>
  readonly entityKey: string
}

export interface Footprint {
  readonly spaceId: Identity.SpaceId
  readonly descriptor: string
  readonly model: string
  readonly index: string
  readonly partition: ReadonlyArray<SqlValue>
  readonly lower: SqlValue | undefined
  readonly lowerInclusive: boolean
  readonly upper: SqlValue | undefined
  readonly upperInclusive: boolean
  readonly direction: SecondaryIndex.Direction
  readonly cursor: ReadonlyArray<SqlValue> | undefined
  readonly boundary: ReadonlyArray<SqlValue> | undefined
  readonly hasMore: boolean
  readonly full: boolean
}

export type ReadRegistration = (
  initial: Footprint
) => Effect.Effect<(complete: Footprint) => Effect.Effect<void>>

const CatalogRow = Schema.Struct({
  layout_hash: Schema.String,
  table_name: Schema.String,
  table_checksum: Schema.String,
  scan_index_name: Schema.String,
  scan_index_checksum: Schema.String
})

const StateRow = Schema.Struct({
  backfill_after_key: Schema.NullOr(Schema.String),
  backfill_visible_revision: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  ready: Schema.Literals([0, 1])
})

const CatalogObjectRow = Schema.Struct({
  model: Schema.String,
  index_name: Schema.String,
  descriptor_hash: Schema.String,
  table_name: Schema.String,
  scan_index_name: Schema.String
})

const ReadyRow = Schema.Struct({
  layout_hash: Schema.String,
  ready: Schema.Literals([0, 1])
})

const GenerationRow = Schema.Struct({
  active_schema_generation: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  active_projection_generation: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  visible_revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})

const IndexedEntityRow = Schema.Struct({
  entity_key: Schema.String,
  value_json: Schema.String
})

const TupleRow = Schema.Struct({ values_json: Schema.String })
const SqliteSchemaRow = Schema.Struct({ sql: Schema.String })
const SqlValuesFromString = Schema.fromJsonString(Schema.Array(Schema.Union([Schema.String, Schema.Number])))

const CursorPayload = Schema.Struct({
  shape: Schema.String,
  values: Schema.Array(Schema.Union([Schema.String, Schema.Number]))
})

const CursorFromString = Schema.fromJsonString(CursorPayload)
const backfillPageSize = 128
const maximumCursorBytes = 16 * 1024

const makeDescriptor = (model: Model.Any, indexName: string, index: SecondaryIndex.Any): Descriptor => {
  const hash = Canonical.hash({
    format: 1,
    model: model.name,
    index: indexName,
    version: index.version,
    partition: index.partition.map((component) => ({
      name: component.name,
      affinity: component.affinity,
      schema: SchemaDescriptor.make(component.schema)
    })),
    sort: index.sort.map((component) => ({
      name: component.name,
      affinity: component.affinity,
      schema: SchemaDescriptor.make(component.schema)
    }))
  })
  const tableName = `effect_local_idx_${hash}`
  const scanIndexName = `${tableName}_scan`
  const componentColumns = [...index.partition, ...index.sort].map((component, position) => {
    let prefix = "s"
    let ordinal = position - index.partition.length
    if (position < index.partition.length) {
      prefix = "p"
      ordinal = position
    }
    return `${prefix}${ordinal} ${affinitySql[component.affinity]} NOT NULL`
  })
  const tableSchema = `CREATE TABLE ${tableName} (
    space_id TEXT NOT NULL,
    index_schema_generation INTEGER NOT NULL CHECK (index_schema_generation >= 0),
    index_projection_generation INTEGER NOT NULL CHECK (index_projection_generation >= 0),
    index_entity_key TEXT NOT NULL,
    ${componentColumns.join(",\n    ")},
    PRIMARY KEY (space_id, index_schema_generation, index_projection_generation, index_entity_key),
    FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
  )`
  const tableDdl = tableSchema.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
  const scanColumns = [
    "space_id",
    "index_schema_generation",
    "index_projection_generation",
    ...index.partition.map((_, position) => `p${position}`),
    ...index.sort.map((_, position) => `s${position}`),
    "index_entity_key"
  ]
  const scanIndexSchema = `CREATE INDEX ${scanIndexName} ON ${tableName} (${scanColumns.join(", ")})`
  const scanIndexDdl = scanIndexSchema.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ")
  return {
    model,
    indexName,
    index,
    hash,
    tableName,
    scanIndexName,
    tableDdl,
    scanIndexDdl,
    tableSchema,
    scanIndexSchema,
    tableChecksum: Canonical.hash({ format: 1, statement: tableSchema }),
    scanIndexChecksum: Canonical.hash({ format: 1, statement: scanIndexSchema })
  }
}

const descriptors = (definition: Definition.Any): ReadonlyArray<Descriptor> =>
  definition.models.flatMap((model) =>
    Object.entries(model.indexes).map(([indexName, index]) => makeDescriptor(model, indexName, index))
  )

const encodedComponents = (
  descriptor: Descriptor,
  value: unknown
): Effect.Effect<ReadonlyArray<SqlValue>, ReplicaError.StorageCorrupt> =>
  Effect.forEach(
    [...descriptor.index.partition, ...descriptor.index.sort],
    (component) => encodeComponent(component, component.extract(value))
  )

const point = (
  address: Address,
  descriptor: Descriptor,
  entityKey: string,
  values: ReadonlyArray<SqlValue>
): Point => ({
  spaceId: address.spaceId,
  descriptor: descriptor.hash,
  model: descriptor.model.name,
  index: descriptor.indexName,
  partition: values.slice(0, descriptor.index.partition.length),
  sort: values.slice(descriptor.index.partition.length),
  entityKey
})

const componentColumns = (descriptor: Descriptor): ReadonlyArray<string> => [
  ...descriptor.index.partition.map((_, position) => `p${position}`),
  ...descriptor.index.sort.map((_, position) => `s${position}`)
]

const makeIndexRow = (
  descriptor: Descriptor,
  address: Address,
  entityKey: string,
  value: unknown
): Effect.Effect<
  { readonly row: Record<string, unknown>; readonly values: ReadonlyArray<SqlValue> },
  ReplicaError.StorageCorrupt
> =>
  Effect.gen(function*() {
    const values = yield* encodedComponents(descriptor, value)
    const row: Record<string, unknown> = {
      space_id: address.spaceId,
      index_schema_generation: address.schemaGeneration,
      index_projection_generation: address.projectionGeneration,
      index_entity_key: entityKey
    }
    for (let position = 0; position < descriptor.index.partition.length; position++) {
      row[`p${position}`] = values[position]
    }
    for (let position = 0; position < descriptor.index.sort.length; position++) {
      row[`s${position}`] = values[descriptor.index.partition.length + position]
    }
    return { row, values }
  })

const upsertRows = (
  sql: SqlClient.SqlClient,
  descriptor: Descriptor,
  rows: ReadonlyArray<Record<string, unknown>>
): Effect.Effect<void, ReplicaError.StorageUnavailable> => {
  if (rows.length === 0) return Effect.void
  const table = sql(descriptor.tableName)
  const columns = componentColumns(descriptor)
  const assignments = sql.join(", ", false)(
    columns.map((name) => sql`${sql(name)} = excluded.${sql(name)}`)
  )
  const batchSize = Math.max(1, Math.floor(900 / (columns.length + 4)))
  return Effect.forEach(
    Array.from(
      { length: Math.ceil(rows.length / batchSize) },
      (_, position) => rows.slice(position * batchSize, (position + 1) * batchSize)
    ),
    (batch) =>
      sql`INSERT INTO ${table} ${sql.insert(batch)}
      ON CONFLICT (space_id, index_schema_generation, index_projection_generation, index_entity_key)
      DO UPDATE SET ${assignments}`,
    { discard: true }
  ).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
}

const writeEntity = (
  sql: SqlClient.SqlClient,
  descriptor: Descriptor,
  address: Address,
  entityKey: string,
  value: unknown
): Effect.Effect<ReadonlyArray<Point>, ReplicaError.StorageError> =>
  Effect.gen(function*() {
    const table = sql(descriptor.tableName)
    const columns = componentColumns(descriptor)
    const findOld = SqlSchema.findOneOption({
      Request: Schema.Struct({
        spaceId: Schema.String,
        schemaGeneration: Schema.Int,
        projectionGeneration: Schema.Int,
        entityKey: Schema.String
      }),
      Result: TupleRow,
      execute: ({ entityKey: storedEntityKey, projectionGeneration, schemaGeneration, spaceId }) =>
        sql`SELECT json_array(${sql.csv(columns.map(sql.literal))}) AS values_json
        FROM ${table} WHERE space_id = ${spaceId} AND index_schema_generation = ${schemaGeneration}
          AND index_projection_generation = ${projectionGeneration} AND index_entity_key = ${storedEntityKey}`
    })
    const old = yield* findOld({ ...address, entityKey }).pipe(
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(new ReplicaError.StorageCorrupt({ message: "Stored secondary index row is invalid", cause }))),
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(StorageUnavailable.make(cause)))
    )
    const points: Array<Point> = []
    if (Option.isSome(old)) {
      const values = yield* Schema.decodeUnknownEffect(SqlValuesFromString)(old.value.values_json).pipe(
        Effect.mapError((cause) =>
          new ReplicaError.StorageCorrupt({ message: "Stored secondary index tuple is invalid", cause })
        )
      )
      points.push(point(address, descriptor, entityKey, values))
    }
    if (value === undefined) {
      yield* sql`DELETE FROM ${table}
        WHERE space_id = ${address.spaceId} AND index_schema_generation = ${address.schemaGeneration}
          AND index_projection_generation = ${address.projectionGeneration} AND index_entity_key = ${entityKey}`
      return points
    }
    const encoded = yield* makeIndexRow(descriptor, address, entityKey, value)
    yield* upsertRows(sql, descriptor, [encoded.row])
    points.push(point(address, descriptor, entityKey, encoded.values))
    return points
  }).pipe(
    Effect.catchTag("SqlError", (cause) =>
      Effect.fail(StorageUnavailable.make(cause))),
    Effect.withSpan("IndexStore.writeEntity", {
      attributes: { "index.model": descriptor.model.name, "index.name": descriptor.indexName }
    })
  )

const readPoint = (
  sql: SqlClient.SqlClient,
  descriptor: Descriptor,
  address: Address,
  entityKey: string
): Effect.Effect<ReadonlyArray<Point>, ReplicaError.StorageError> => {
  const table = sql(descriptor.tableName)
  const columns = componentColumns(descriptor)
  return SqlSchema.findOneOption({
    Request: Schema.Void,
    Result: TupleRow,
    execute: () =>
      sql`SELECT json_array(${sql.csv(columns.map(sql.literal))}) AS values_json FROM ${table}
      WHERE space_id = ${address.spaceId} AND index_schema_generation = ${address.schemaGeneration}
        AND index_projection_generation = ${address.projectionGeneration} AND index_entity_key = ${entityKey}`
  })(undefined).pipe(
    Effect.catchTag(
      "SchemaError",
      (cause) =>
        Effect.fail(new ReplicaError.StorageCorrupt({ message: "Stored secondary index row is invalid", cause }))
    ),
    Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
    Effect.flatMap(Option.match({
      onNone: () => Effect.succeed([]),
      onSome: (row) =>
        Schema.decodeUnknownEffect(SqlValuesFromString)(row.values_json).pipe(
          Effect.mapError((cause) =>
            new ReplicaError.StorageCorrupt({ message: "Stored secondary index tuple is invalid", cause })
          ),
          Effect.map((values) => [point(address, descriptor, entityKey, values)])
        )
    }))
  )
}

const decodeEntity = (model: Model.Any, valueJson: string) =>
  Codec.parse(valueJson).pipe(Effect.flatMap((encoded) => Codec.decode(model.schema, encoded)))

const ensureCatalog = (
  sql: SqlClient.SqlClient,
  descriptor: Descriptor
): Effect.Effect<void, ReplicaError.StorageError> => {
  const findCatalog = SqlSchema.findOneOption({
    Request: Schema.Struct({ model: Schema.String, indexName: Schema.String, hash: Schema.String }),
    Result: CatalogRow,
    execute: ({ hash, indexName, model }) =>
      sql`SELECT layout_hash, table_name, table_checksum, scan_index_name, scan_index_checksum
      FROM effect_local_client_index_catalog
      WHERE model = ${model} AND index_name = ${indexName} AND descriptor_hash = ${hash}`
  })
  const findSchema = SqlSchema.findOneOption({
    Request: Schema.Struct({ type: Schema.String, name: Schema.String }),
    Result: SqliteSchemaRow,
    execute: ({ name, type }) => sql`SELECT sql FROM sqlite_schema WHERE type = ${type} AND name = ${name}`
  })
  return sql.withTransaction(Effect.gen(function*() {
    const stored = yield* findCatalog({
      model: descriptor.model.name,
      indexName: descriptor.indexName,
      hash: descriptor.hash
    }).pipe(Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(
        new ReplicaError.StorageCorrupt({
          message: `Secondary index catalog row is invalid for ${descriptor.model.name}.${descriptor.indexName}`,
          cause
        })
      )))
    const priorTable = yield* findSchema({ type: "table", name: descriptor.tableName }).pipe(
      Effect.catchTag(
        "SchemaError",
        (cause) =>
          Effect.fail(new ReplicaError.StorageCorrupt({ message: "SQLite table catalog row is invalid", cause }))
      )
    )
    yield* sql.unsafe(descriptor.tableDdl)
    yield* sql.unsafe(descriptor.scanIndexDdl)
    const tableSchema = yield* findSchema({ type: "table", name: descriptor.tableName }).pipe(
      Effect.catchTag(
        "SchemaError",
        (cause) =>
          Effect.fail(new ReplicaError.StorageCorrupt({ message: "SQLite table catalog row is invalid", cause }))
      )
    )
    const scanIndexSchema = yield* findSchema({ type: "index", name: descriptor.scanIndexName }).pipe(
      Effect.catchTag(
        "SchemaError",
        (cause) =>
          Effect.fail(new ReplicaError.StorageCorrupt({ message: "SQLite index catalog row is invalid", cause }))
      )
    )
    if (Option.isNone(tableSchema) || Option.isNone(scanIndexSchema)) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Secondary index SQLite objects are missing for ${descriptor.model.name}.${descriptor.indexName}`
      })
    }
    if (
      Canonical.hash({ format: 1, statement: tableSchema.value.sql }) !== descriptor.tableChecksum ||
      Canonical.hash({ format: 1, statement: scanIndexSchema.value.sql }) !== descriptor.scanIndexChecksum
    ) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Secondary index SQLite schema mismatch for ${descriptor.model.name}.${descriptor.indexName}`
      })
    }
    if (Option.isNone(stored)) {
      yield* sql`INSERT INTO effect_local_client_index_catalog
        (model, index_name, descriptor_hash, layout_hash, table_name, table_checksum,
          scan_index_name, scan_index_checksum)
        VALUES (${descriptor.model.name}, ${descriptor.indexName}, ${descriptor.hash}, ${descriptor.hash},
          ${descriptor.tableName}, ${descriptor.tableChecksum}, ${descriptor.scanIndexName},
          ${descriptor.scanIndexChecksum})`
      return yield* Effect.void
    }
    const row = stored.value
    if (
      row.layout_hash !== descriptor.hash || row.table_name !== descriptor.tableName ||
      row.table_checksum !== descriptor.tableChecksum || row.scan_index_name !== descriptor.scanIndexName ||
      row.scan_index_checksum !== descriptor.scanIndexChecksum
    ) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Secondary index catalog checksum mismatch for ${descriptor.model.name}.${descriptor.indexName}`
      })
    }
    if (Option.isNone(priorTable)) {
      yield* sql`UPDATE effect_local_client_index_state SET ready = 0, backfill_after_key = NULL,
        backfill_visible_revision = NULL WHERE model = ${descriptor.model.name}
          AND index_name = ${descriptor.indexName} AND descriptor_hash = ${descriptor.hash}`
    }
    return yield* Effect.void
  })).pipe(
    Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
    Effect.withSpan("IndexStore.ensureCatalog", {
      attributes: { "index.model": descriptor.model.name, "index.name": descriptor.indexName }
    })
  )
}

const findGeneration = (sql: SqlClient.SqlClient, spaceId: Identity.SpaceId) =>
  SqlSchema.findOne({
    Request: Schema.Struct({ spaceId: Schema.String }),
    Result: GenerationRow,
    execute: ({ spaceId: requestedSpaceId }) =>
      sql`SELECT active_schema_generation, active_projection_generation, visible_revision
      FROM effect_local_client_spaces WHERE space_id = ${requestedSpaceId}`
  })({ spaceId }).pipe(
    Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "Client generation metadata is invalid", cause }))),
    Effect.catchTag("SqlError", (cause) =>
      Effect.fail(StorageUnavailable.make(cause))),
    Effect.catchTag("NoSuchElementError", () =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "Client generation metadata is missing" })))
  )

const sameAddress = (row: typeof GenerationRow.Type, address: Address): boolean =>
  row.active_schema_generation === address.schemaGeneration &&
  row.active_projection_generation === address.projectionGeneration

const backfill = (
  sql: SqlClient.SqlClient,
  descriptor: Descriptor,
  address: Address
): Effect.Effect<void, ReplicaError.StorageError> =>
  Effect.gen(function*() {
    const findState = SqlSchema.findOne({
      Request: Schema.Struct({
        spaceId: Schema.String,
        schemaGeneration: Schema.Int,
        projectionGeneration: Schema.Int,
        model: Schema.String,
        indexName: Schema.String,
        hash: Schema.String
      }),
      Result: StateRow,
      execute: ({ hash, indexName, model, projectionGeneration, schemaGeneration, spaceId }) =>
        sql`SELECT backfill_after_key, backfill_visible_revision, ready
        FROM effect_local_client_index_state WHERE space_id = ${spaceId}
          AND schema_generation = ${schemaGeneration} AND projection_generation = ${projectionGeneration}
          AND model = ${model} AND index_name = ${indexName} AND descriptor_hash = ${hash}`
    })
    const stateRequest = {
      ...address,
      model: descriptor.model.name,
      indexName: descriptor.indexName,
      hash: descriptor.hash
    }
    yield* sql`INSERT INTO effect_local_client_index_state
      (space_id, schema_generation, projection_generation, model, index_name, descriptor_hash)
      VALUES (${address.spaceId}, ${address.schemaGeneration}, ${address.projectionGeneration},
        ${descriptor.model.name}, ${descriptor.indexName}, ${descriptor.hash})
      ON CONFLICT DO NOTHING`
    let state = yield* findState(stateRequest).pipe(
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(new ReplicaError.StorageCorrupt({ message: "Secondary index state row is invalid", cause }))),
      Effect.catchTag("NoSuchElementError", (cause) =>
        Effect.fail(new ReplicaError.StorageCorrupt({ message: "Secondary index state row is missing", cause })))
    )
    const startingMeta = yield* findGeneration(sql, address.spaceId)
    if (!sameAddress(startingMeta, address)) {
      return yield* new ReplicaError.StorageCorrupt({
        message: "Secondary index address does not match the active projection"
      })
    }
    if (state.ready === 1) {
      return yield* Effect.void
    }
    let revision = state.backfill_visible_revision
    let after = state.backfill_after_key
    if (revision !== startingMeta.visible_revision) {
      revision = startingMeta.visible_revision
      after = null
      yield* sql.withTransaction(Effect.gen(function*() {
        const current = yield* findGeneration(sql, address.spaceId)
        if (!sameAddress(current, address)) {
          return yield* new ReplicaError.StorageCorrupt({
            message: "Projection changed while secondary indexes were backfilling"
          })
        }
        revision = current.visible_revision
        const table = sql(descriptor.tableName)
        yield* sql`DELETE FROM ${table} WHERE space_id = ${address.spaceId}
          AND index_schema_generation = ${address.schemaGeneration}
          AND index_projection_generation = ${address.projectionGeneration}`
        yield* sql`UPDATE effect_local_client_index_state
          SET backfill_after_key = NULL, backfill_visible_revision = ${revision}, ready = 0
          WHERE space_id = ${address.spaceId} AND schema_generation = ${address.schemaGeneration}
            AND projection_generation = ${address.projectionGeneration} AND model = ${descriptor.model.name}
            AND index_name = ${descriptor.indexName} AND descriptor_hash = ${descriptor.hash}`
        return yield* Effect.void
      }))
    }
    const findPage = SqlSchema.findAll({
      Request: Schema.Struct({
        spaceId: Schema.String,
        schemaGeneration: Schema.Int,
        projectionGeneration: Schema.Int,
        model: Schema.String,
        after: Schema.NullOr(Schema.String)
      }),
      Result: IndexedEntityRow,
      execute: ({ after: pageAfter, model, projectionGeneration, schemaGeneration, spaceId }) => {
        if (pageAfter === null) {
          return sql`SELECT entity_key, value_json FROM effect_local_client_visible_entities_data
            WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
              AND projection_generation = ${projectionGeneration} AND model = ${model}
            ORDER BY entity_key LIMIT ${backfillPageSize}`
        }
        return sql`SELECT entity_key, value_json FROM effect_local_client_visible_entities_data
          WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
            AND projection_generation = ${projectionGeneration} AND model = ${model} AND entity_key > ${pageAfter}
          ORDER BY entity_key LIMIT ${backfillPageSize}`
      }
    })
    while (true) {
      const result = yield* sql.withTransaction(Effect.gen(function*() {
        const current = yield* findGeneration(sql, address.spaceId)
        if (!sameAddress(current, address)) {
          return yield* new ReplicaError.StorageCorrupt({
            message: "Projection changed while secondary indexes were backfilling"
          })
        }
        if (current.visible_revision !== revision) {
          const table = sql(descriptor.tableName)
          revision = current.visible_revision
          after = null
          yield* sql`DELETE FROM ${table} WHERE space_id = ${address.spaceId}
            AND index_schema_generation = ${address.schemaGeneration}
            AND index_projection_generation = ${address.projectionGeneration}`
          yield* sql`UPDATE effect_local_client_index_state
            SET backfill_after_key = NULL, backfill_visible_revision = ${revision}, ready = 0
            WHERE space_id = ${address.spaceId} AND schema_generation = ${address.schemaGeneration}
              AND projection_generation = ${address.projectionGeneration} AND model = ${descriptor.model.name}
              AND index_name = ${descriptor.indexName} AND descriptor_hash = ${descriptor.hash}`
          return { _tag: "Restart" as const }
        }
        const rows = yield* findPage({ ...address, model: descriptor.model.name, after }).pipe(
          Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(new ReplicaError.StorageCorrupt({ message: "Secondary index backfill row is invalid", cause })))
        )
        if (rows.length === 0) {
          const table = sql(descriptor.tableName)
          yield* sql`UPDATE effect_local_client_index_state
            SET backfill_after_key = NULL, backfill_visible_revision = NULL, ready = 1
            WHERE space_id = ${address.spaceId} AND schema_generation = ${address.schemaGeneration}
              AND projection_generation = ${address.projectionGeneration} AND model = ${descriptor.model.name}
              AND index_name = ${descriptor.indexName} AND descriptor_hash = ${descriptor.hash}
              AND backfill_visible_revision = ${revision}`
          yield* sql`DELETE FROM ${table} WHERE space_id = ${address.spaceId} AND NOT
            (index_schema_generation = ${address.schemaGeneration}
              AND index_projection_generation = ${address.projectionGeneration})`
          yield* sql`DELETE FROM effect_local_client_index_state WHERE space_id = ${address.spaceId}
            AND model = ${descriptor.model.name} AND index_name = ${descriptor.indexName}
            AND descriptor_hash = ${descriptor.hash} AND NOT
              (schema_generation = ${address.schemaGeneration}
                AND projection_generation = ${address.projectionGeneration})`
          return { _tag: "Done" as const }
        }
        const indexRows: Array<Record<string, unknown>> = []
        for (const row of rows) {
          const value = yield* decodeEntity(descriptor.model, row.value_json)
          indexRows.push((yield* makeIndexRow(descriptor, address, row.entity_key, value)).row)
        }
        yield* upsertRows(sql, descriptor, indexRows)
        const nextAfter = rows[rows.length - 1].entity_key
        yield* sql`UPDATE effect_local_client_index_state SET backfill_after_key = ${nextAfter}
          WHERE space_id = ${address.spaceId} AND schema_generation = ${address.schemaGeneration}
            AND projection_generation = ${address.projectionGeneration} AND model = ${descriptor.model.name}
            AND index_name = ${descriptor.indexName} AND descriptor_hash = ${descriptor.hash}
            AND backfill_visible_revision = ${revision}`
        return { _tag: "Page" as const, after: nextAfter }
      }))
      if (result._tag === "Done") {
        return yield* Effect.void
      }
      if (result._tag === "Page") {
        after = result.after
      }
    }
  }).pipe(
    Effect.catchTag("SqlError", (cause) =>
      Effect.fail(StorageUnavailable.make(cause))),
    Effect.withSpan("IndexStore.backfill", {
      attributes: { "index.model": descriptor.model.name, "index.name": descriptor.indexName }
    })
  )

const cleanupObsolete = (
  sql: SqlClient.SqlClient,
  current: ReadonlyArray<Descriptor>
): Effect.Effect<void, ReplicaError.StorageError> =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: CatalogObjectRow,
    execute: () =>
      sql`SELECT model, index_name, descriptor_hash, table_name, scan_index_name
      FROM effect_local_client_index_catalog`
  })(undefined).pipe(
    Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "Secondary index catalog row is invalid", cause }))),
    Effect.flatMap((rows) => {
      const hashes = new Set(current.map((descriptor) =>
        descriptor.hash
      ))
      const obsolete = rows.filter((row) => !hashes.has(row.descriptor_hash))
      return sql.withTransaction(Effect.forEach(obsolete, (row) =>
        Effect.gen(function*() {
          yield* sql`DROP INDEX IF EXISTS ${sql(row.scan_index_name)}`
          yield* sql`DROP TABLE IF EXISTS ${sql(row.table_name)}`
          yield* sql`DELETE FROM effect_local_client_index_catalog
            WHERE model = ${row.model} AND index_name = ${row.index_name}
              AND descriptor_hash = ${row.descriptor_hash}`
        }), { discard: true }))
    }),
    Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
    Effect.withSpan("IndexStore.cleanupObsolete")
  )

export interface Runtime {
  readonly ensure: (address: Address) => Effect.Effect<void, ReplicaError.StorageError>
  readonly points: (
    address: Address,
    entities: ReadonlyArray<EntityIdentity>
  ) => Effect.Effect<ReadonlyArray<Point>, ReplicaError.StorageError>
  readonly replace: (
    address: Address,
    entities: ReadonlyArray<EntityIdentity>
  ) => Effect.Effect<ReadonlyArray<Point>, ReplicaError.StorageError>
}

export interface EntityIdentity {
  readonly model: string
  readonly entityKey: string
}

export const install = (
  sql: SqlClient.SqlClient,
  definition: Definition.Any,
  initial: Address
): Effect.Effect<Runtime, ReplicaError.StorageError> =>
  Effect.gen(function*() {
    const all = descriptors(definition)
    const byModel = new Map<string, ReadonlyArray<Descriptor>>()
    for (const model of definition.models) {
      byModel.set(model.name, all.filter((descriptor) => descriptor.model === model))
    }
    for (const descriptor of all) yield* ensureCatalog(sql, descriptor)
    yield* cleanupObsolete(sql, all)
    const ensure: Runtime["ensure"] = (address) =>
      Effect.forEach(all, (descriptor) => backfill(sql, descriptor, address), { discard: true }).pipe(
        Effect.withSpan("IndexStore.ensure", { attributes: { "space.id": address.spaceId } })
      )
    const points: Runtime["points"] = (address, entities) =>
      Effect.forEach(entities, (entity) =>
        Effect.forEach(
          byModel.get(entity.model) ?? [],
          (descriptor) => readPoint(sql, descriptor, address, entity.entityKey)
        ).pipe(Effect.map((values) => values.flat()))).pipe(Effect.map((values) => values.flat()))
    const findEntity = SqlSchema.findOneOption({
      Request: Schema.Struct({
        spaceId: Schema.String,
        schemaGeneration: Schema.Int,
        projectionGeneration: Schema.Int,
        model: Schema.String,
        entityKey: Schema.String
      }),
      Result: IndexedEntityRow,
      execute: ({ entityKey, model, projectionGeneration, schemaGeneration, spaceId }) =>
        sql`SELECT entity_key, value_json FROM effect_local_client_visible_entities_data
        WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
          AND projection_generation = ${projectionGeneration} AND model = ${model} AND entity_key = ${entityKey}`
    })
    const replace: Runtime["replace"] = (address, entities) =>
      Effect.forEach(entities, (entity) =>
        Effect.gen(function*() {
          const model = definition.modelByName.get(entity.model)
          if (model === undefined) {
            return yield* new ReplicaError.StorageCorrupt({ message: `Unknown indexed model: ${entity.model}` })
          }
          const found = yield* findEntity({ ...address, ...entity }).pipe(
            Effect.catchTag(
              "SchemaError",
              (cause) =>
                Effect.fail(new ReplicaError.StorageCorrupt({ message: "Indexed entity row is invalid", cause }))
            ),
            Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
          )
          let value: unknown
          if (Option.isSome(found)) value = yield* decodeEntity(model, found.value.value_json)
          return yield* Effect.forEach(
            byModel.get(entity.model) ?? [],
            (descriptor) => writeEntity(sql, descriptor, address, entity.entityKey, value)
          ).pipe(Effect.map((values) => values.flat()))
        })).pipe(
          Effect.map((values) => values.flat()),
          Effect.withSpan("IndexStore.replace", { attributes: { "space.id": address.spaceId } })
        )
    const runtime = { ensure, points, replace }
    yield* runtime.ensure(initial)
    return runtime
  }).pipe(Effect.withSpan("IndexStore.install"))

interface QueryState {
  readonly where: Readonly<Record<string, unknown>> | undefined
  readonly direction: SecondaryIndex.Direction
  readonly limit: number
  readonly cursor: string | undefined
}

const column = (sql: SqlClient.SqlClient, name: string) => sql.literal(`i.${name}`)

const encodeWhere = (
  descriptor: Descriptor,
  where: Readonly<Record<string, unknown>> | undefined
): Effect.Effect<{
  readonly shape: Readonly<Record<string, unknown>>
}, ReplicaError.StorageCorrupt> =>
  Effect.gen(function*() {
    const shape: Record<string, unknown> = {}
    for (let position = 0; position < descriptor.index.partition.length; position++) {
      const component = descriptor.index.partition[position]
      let value: unknown
      if (where !== undefined) value = Reflect.get(where, component.name)
      if (value === undefined) {
        return yield* new ReplicaError.StorageCorrupt({ message: `Query is missing partition ${component.name}` })
      }
      const encoded = yield* encodeComponent(component, value)
      shape[component.name] = encoded
    }
    const firstSort = descriptor.index.sort[0]
    if (firstSort !== undefined && where !== undefined) {
      const bounds = Reflect.get(where, firstSort.name)
      if (bounds !== undefined) {
        if (bounds === null || typeof bounds !== "object") {
          return yield* new ReplicaError.StorageCorrupt({
            message: `Bounds for ${firstSort.name} must be an object`
          })
        }
        const encodedBounds: Record<string, SqlValue> = {}
        for (const operator of ["gt", "gte", "lt", "lte"] as const) {
          const value = Reflect.get(bounds, operator)
          if (value !== undefined) encodedBounds[operator] = yield* encodeComponent(firstSort, value)
        }
        if (encodedBounds.gt !== undefined && encodedBounds.gte !== undefined) {
          return yield* new ReplicaError.StorageCorrupt({
            message: `Bounds for ${firstSort.name} cannot contain both gt and gte`
          })
        }
        if (encodedBounds.lt !== undefined && encodedBounds.lte !== undefined) {
          return yield* new ReplicaError.StorageCorrupt({
            message: `Bounds for ${firstSort.name} cannot contain both lt and lte`
          })
        }
        shape[firstSort.name] = encodedBounds
      }
    }
    return { shape }
  })

const runPage = <M extends Model.Any, Name extends keyof Model.Indexes<M> & string,>(
  sql: SqlClient.SqlClient,
  address: Address,
  model: M,
  indexName: Name,
  descriptor: Descriptor,
  state: QueryState,
  onRead?: ReadRegistration
): Effect.Effect<
  SecondaryIndex.Page<Model.Value<M>, SecondaryIndex.Cursor<M["name"], Name>>,
  ReplicaError.StorageError
> =>
  Effect.gen(function*() {
    yield* backfill(sql, descriptor, address)
    if (state.direction !== "asc" && state.direction !== "desc") {
      return yield* new ReplicaError.StorageCorrupt({ message: "Query order must be asc or desc" })
    }
    if (!Number.isSafeInteger(state.limit) || state.limit <= 0 || state.limit > 1_000) {
      return yield* new ReplicaError.StorageCorrupt({
        message: "Query limit must be a positive safe integer no greater than 1000"
      })
    }
    const ready = yield* SqlSchema.findOneOption({
      Request: Schema.Struct({
        spaceId: Schema.String,
        schemaGeneration: Schema.Int,
        projectionGeneration: Schema.Int,
        model: Schema.String,
        indexName: Schema.String,
        hash: Schema.String
      }),
      Result: ReadyRow,
      execute: ({
        hash,
        indexName: requestedIndex,
        model: requestedModel,
        projectionGeneration,
        schemaGeneration,
        spaceId
      }) =>
        sql`SELECT c.layout_hash, s.ready FROM effect_local_client_index_catalog AS c
        INNER JOIN effect_local_client_index_state AS s
          ON s.model = c.model AND s.index_name = c.index_name AND s.descriptor_hash = c.descriptor_hash
        WHERE s.space_id = ${spaceId} AND s.schema_generation = ${schemaGeneration}
          AND s.projection_generation = ${projectionGeneration} AND c.model = ${requestedModel}
          AND c.index_name = ${requestedIndex} AND c.descriptor_hash = ${hash}`
    })({ ...address, model: model.name, indexName, hash: descriptor.hash }).pipe(
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(new ReplicaError.StorageCorrupt({ message: "Secondary index readiness row is invalid", cause }))),
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(StorageUnavailable.make(cause)))
    )
    if (
      Option.isNone(ready) || ready.value.layout_hash !== descriptor.hash ||
      ready.value.ready !== 1
    ) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Secondary index ${model.name}.${indexName} is not ready for this projection`
      })
    }
    const encodedWhere = yield* encodeWhere(descriptor, state.where)
    const whereClauses: Array<Statement.Fragment> = []
    for (let position = 0; position < descriptor.index.partition.length; position++) {
      const component = descriptor.index.partition[position]
      whereClauses.push(sql`${column(sql, `p${position}`)} = ${Reflect.get(encodedWhere.shape, component.name)}`)
    }
    const firstSort = descriptor.index.sort[0]
    if (firstSort !== undefined) {
      const bounds = Reflect.get(encodedWhere.shape, firstSort.name)
      if (bounds !== undefined && bounds !== null && typeof bounds === "object") {
        for (const [operator, token] of [["gt", ">"], ["gte", ">="], ["lt", "<"], ["lte", "<="]] as const) {
          const value = Reflect.get(bounds, operator)
          if (value !== undefined) {
            whereClauses.push(sql`${column(sql, "s0")} ${sql.literal(token)} ${value}`)
          }
        }
      }
    }
    const shape = Canonical.hash({
      format: 1,
      descriptor: descriptor.hash,
      where: encodedWhere.shape,
      direction: state.direction,
      limit: state.limit
    })
    const orderedColumns = [
      ...descriptor.index.sort.map((_, position) =>
        `s${position}`
      ),
      "index_entity_key"
    ]
    let cursorValues: ReadonlyArray<SqlValue> | undefined
    if (state.cursor !== undefined) {
      if (
        typeof state.cursor !== "string" || state.cursor.length > maximumCursorBytes ||
        new TextEncoder().encode(state.cursor).byteLength > maximumCursorBytes
      ) {
        return yield* new ReplicaError.StorageCorrupt({
          message: "Query cursor exceeds the maximum encoded size"
        })
      }
      const cursor = yield* Schema.decodeUnknownEffect(CursorFromString)(state.cursor).pipe(
        Effect.mapError((cause) => new ReplicaError.StorageCorrupt({ message: "Query cursor is invalid", cause }))
      )
      if (cursor.shape !== shape || cursor.values.length !== orderedColumns.length) {
        return yield* new ReplicaError.StorageCorrupt({ message: "Query cursor does not belong to this query shape" })
      }
      for (let position = 0; position < descriptor.index.sort.length; position++) {
        const component = descriptor.index.sort[position]
        const value = cursor.values[position]
        let valid = typeof value === "string"
        if (component.affinity === "real") valid = typeof value === "number" && Number.isFinite(value)
        if (component.affinity === "integer") valid = typeof value === "number" && Number.isSafeInteger(value)
        if (!valid) {
          return yield* new ReplicaError.StorageCorrupt({
            message: `Query cursor value for ${component.name} does not match its SQLite affinity`
          })
        }
      }
      if (typeof cursor.values[cursor.values.length - 1] !== "string") {
        return yield* new ReplicaError.StorageCorrupt({ message: "Query cursor entity key must be a string" })
      }
      cursorValues = cursor.values
      const tupleColumns = sql.csv(orderedColumns.map((name) => column(sql, name)))
      const tupleValues = sql.csv(cursor.values.map((value) => sql`${value}`))
      let token = "<"
      if (state.direction === "asc") token = ">"
      whereClauses.push(sql`(${tupleColumns}) ${sql.literal(token)} (${tupleValues})`)
    }
    const partition = descriptor.index.partition.map((component) => Reflect.get(encodedWhere.shape, component.name))
      .filter((value): value is SqlValue => typeof value === "string" || typeof value === "number")
    let lower: SqlValue | undefined
    let lowerInclusive = false
    let upper: SqlValue | undefined
    let upperInclusive = false
    if (firstSort !== undefined) {
      const bounds = Reflect.get(encodedWhere.shape, firstSort.name)
      if (bounds !== undefined && bounds !== null && typeof bounds === "object") {
        const gt = Reflect.get(bounds, "gt")
        const gte = Reflect.get(bounds, "gte")
        const lt = Reflect.get(bounds, "lt")
        const lte = Reflect.get(bounds, "lte")
        if (typeof gt === "string" || typeof gt === "number") lower = gt
        if (typeof gte === "string" || typeof gte === "number") {
          lower = gte
          lowerInclusive = true
        }
        if (typeof lt === "string" || typeof lt === "number") upper = lt
        if (typeof lte === "string" || typeof lte === "number") {
          upper = lte
          upperInclusive = true
        }
      }
    }
    const makeFootprint = (
      boundary: ReadonlyArray<SqlValue> | undefined,
      hasMore: boolean,
      full: boolean
    ): Footprint => ({
      spaceId: address.spaceId,
      descriptor: descriptor.hash,
      model: model.name,
      index: indexName,
      partition,
      lower,
      lowerInclusive,
      upper,
      upperInclusive,
      direction: state.direction,
      cursor: cursorValues,
      boundary,
      hasMore,
      full
    })
    let completeRead: ((footprint: Footprint) => Effect.Effect<void>) | undefined
    if (onRead !== undefined) completeRead = yield* onRead(makeFootprint(undefined, false, false))
    const table = sql(descriptor.tableName)
    let orderDirection = sql.literal("ASC")
    if (state.direction === "desc") orderDirection = sql.literal("DESC")
    const order = sql.csv(
      orderedColumns.map((name) => sql`${column(sql, name)} ${orderDirection}`)
    )
    const findRows = SqlSchema.findAll({
      Request: Schema.Void,
      Result: IndexedEntityRow,
      execute: () =>
        sql`SELECT e.entity_key, e.value_json FROM ${table} AS i
        INNER JOIN effect_local_client_visible_entities_data AS e
          ON e.space_id = i.space_id AND e.schema_generation = i.index_schema_generation
          AND e.projection_generation = i.index_projection_generation AND e.model = ${model.name}
          AND e.entity_key = i.index_entity_key
        WHERE i.space_id = ${address.spaceId} AND i.index_schema_generation = ${address.schemaGeneration}
          AND i.index_projection_generation = ${address.projectionGeneration} AND ${sql.and(whereClauses)}
        ORDER BY ${order} LIMIT ${state.limit + 1}`
    })
    const rows = yield* findRows(undefined).pipe(
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(new ReplicaError.StorageCorrupt({ message: "Secondary index query row is invalid", cause }))),
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(StorageUnavailable.make(cause)))
    )
    const pageRows = rows.slice(0, state.limit)
    const items = yield* Effect.forEach(pageRows, (row) =>
      decodeEntity(model, row.value_json))
    let next: SecondaryIndex.Cursor<M["name"], Name> | undefined
    if (rows.length > state.limit && pageRows.length > 0) {
      const lastRow = pageRows[pageRows.length - 1]
      const lastValue = items[items.length - 1]
      const values = yield* Effect.forEach(
        descriptor.index.sort,
        (component) => encodeComponent(component, component.extract(lastValue))
      )
      const token = yield* Schema.encodeEffect(CursorFromString)({
        shape,
        values: [...values, lastRow.entity_key]
      }).pipe(
        Effect.mapError((cause) =>
          new ReplicaError.StorageCorrupt({ message: "Query cursor could not be encoded", cause })
        )
      )
      next = { model: model.name, index: indexName, token }
    }
    if (completeRead !== undefined) {
      let boundary: ReadonlyArray<SqlValue> | undefined
      if (pageRows.length > 0) {
        const lastRow = pageRows[pageRows.length - 1]
        const lastValue = items[items.length - 1]
        const values = yield* Effect.forEach(
          descriptor.index.sort,
          (component) => encodeComponent(component, component.extract(lastValue))
        )
        boundary = [...values, lastRow.entity_key]
      }
      yield* completeRead(makeFootprint(boundary, rows.length > state.limit, pageRows.length === state.limit))
    }
    return { items, next }
  }).pipe(
    Effect.withSpan("IndexStore.queryPage", {
      attributes: { "index.model": model.name, "index.name": indexName }
    })
  )

export const query = <M extends Model.Any, Name extends keyof Model.Indexes<M> & string,>(
  sql: SqlClient.SqlClient,
  address: Address,
  model: M,
  indexName: Name,
  onRead?: ReadRegistration
): SecondaryIndex.Builder<M["name"], Name, Model.Value<M>, Model.Indexes<M>[Name]> => {
  const index = model.indexes[indexName]
  const descriptor = makeDescriptor(model, indexName, index)
  const build = (
    state: QueryState
  ): SecondaryIndex.Builder<M["name"], Name, Model.Value<M>, Model.Indexes<M>[Name]> => ({
    where: (where) => build({ ...state, where }),
    order: (direction) => build({ ...state, direction }),
    limit: (limit) => build({ ...state, limit }),
    after: (cursor) => build({ ...state, cursor: cursor.token }),
    page: () => runPage(sql, address, model, indexName, descriptor, state, onRead),
    stream: () => {
      let registered = false
      let streamRead: ReadRegistration | undefined
      if (onRead !== undefined) {
        streamRead = (initial) => {
          if (registered) return Effect.succeed(() => Effect.void)
          registered = true
          return onRead({ ...initial, boundary: undefined, hasMore: false, full: true }).pipe(
            Effect.as(() => Effect.void)
          )
        }
      }
      return Stream.paginate(
        state.cursor,
        (cursor) =>
          runPage(sql, address, model, indexName, descriptor, { ...state, cursor }, streamRead).pipe(
            Effect.map((page) => {
              if (page.next === undefined) return [page.items, Option.none<string>()] as const
              return [page.items, Option.some(page.next.token)] as const
            })
          )
      )
    }
  })
  return build({ where: undefined, direction: "asc", limit: 50, cursor: undefined })
}
