import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Model from "@lucas-barake/effect-local/Model"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as SchemaDescriptor from "@lucas-barake/effect-local/SchemaDescriptor"
import type * as SecondaryIndex from "@lucas-barake/effect-local/SecondaryIndex"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import type * as Statement from "effect/unstable/sql/Statement"
import * as Codec from "./internal/codec.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

type SqlValue = string | number

interface Descriptor {
  readonly model: Model.Any
  readonly indexName: string
  readonly index: SecondaryIndex.Any
  readonly hash: string
  readonly tableName: string
  readonly scanIndexName: string
  readonly tableDdl: string
  readonly scanIndexDdl: string
  readonly tableChecksum: string
  readonly scanIndexChecksum: string
}

const CatalogRow = Schema.Struct({
  layout_hash: Schema.String,
  table_name: Schema.String,
  table_checksum: Schema.String,
  scan_index_name: Schema.String,
  scan_index_checksum: Schema.String,
  backfill_generation: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  backfill_after_key: Schema.NullOr(Schema.String),
  ready_generation: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)))
})

const ReadyRow = Schema.Struct({
  layout_hash: Schema.String,
  ready_generation: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)))
})

const GenerationRow = Schema.Struct({
  active_schema_generation: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})

const IndexedEntityRow = Schema.Struct({
  entity_key: Schema.String,
  value_json: Schema.String
})

const CursorPayload = Schema.Struct({
  shape: Schema.String,
  values: Schema.Array(Schema.Union([Schema.String, Schema.Number]))
})

const CursorFromString = Schema.fromJsonString(CursorPayload)
const affinitySql = { text: "TEXT", real: "REAL", integer: "INTEGER" } as const
const backfillPageSize = 128

const storageCorrupt = (message: string, cause?: unknown) => {
  if (cause === undefined) return new ReplicaError.StorageCorrupt({ message })
  return new ReplicaError.StorageCorrupt({ message, cause })
}

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
  const tableDdl = `CREATE TABLE IF NOT EXISTS ${tableName} (
    index_generation INTEGER NOT NULL CHECK (index_generation >= 0),
    index_entity_key TEXT NOT NULL,
    ${componentColumns.join(",\n    ")},
    PRIMARY KEY (index_generation, index_entity_key)
  )`
  const scanColumns = [
    "index_generation",
    ...index.partition.map((_, position) => `p${position}`),
    ...index.sort.map((_, position) => `s${position}`),
    "index_entity_key"
  ]
  const scanIndexDdl = `CREATE INDEX IF NOT EXISTS ${scanIndexName} ON ${tableName} (${scanColumns.join(", ")})`
  return {
    model,
    indexName,
    index,
    hash,
    tableName,
    scanIndexName,
    tableDdl,
    scanIndexDdl,
    tableChecksum: Canonical.hash({ format: 1, statement: tableDdl }),
    scanIndexChecksum: Canonical.hash({ format: 1, statement: scanIndexDdl })
  }
}

const descriptors = (definition: Definition.Any): ReadonlyArray<Descriptor> =>
  definition.models.flatMap((model) =>
    Object.entries(model.indexes).map(([indexName, index]) => makeDescriptor(model, indexName, index))
  )

const encodeComponent = (
  component: SecondaryIndex.ComponentInput,
  value: unknown
): Effect.Effect<SqlValue, ReplicaError.StorageCorrupt> =>
  Schema.encodeUnknownEffect(component.schema)(value).pipe(
    Effect.mapError((cause) => storageCorrupt(`Index component ${component.name} failed Schema encoding`, cause)),
    Effect.flatMap((encoded): Effect.Effect<SqlValue, ReplicaError.StorageCorrupt> => {
      if (component.affinity === "text" && typeof encoded === "string") return Effect.succeed<SqlValue>(encoded)
      if (component.affinity === "real" && typeof encoded === "number" && Number.isFinite(encoded)) {
        return Effect.succeed<SqlValue>(encoded)
      }
      if (component.affinity === "integer") {
        if (typeof encoded === "boolean") {
          if (encoded) return Effect.succeed<SqlValue>(1)
          return Effect.succeed<SqlValue>(0)
        }
        if (typeof encoded === "number" && Number.isSafeInteger(encoded)) return Effect.succeed<SqlValue>(encoded)
      }
      return Effect.fail(storageCorrupt(`Index component ${component.name} encoded outside its SQLite affinity`))
    })
  )

const encodedComponents = (
  descriptor: Descriptor,
  value: unknown
): Effect.Effect<ReadonlyArray<SqlValue>, ReplicaError.StorageCorrupt> =>
  Effect.forEach(
    [...descriptor.index.partition, ...descriptor.index.sort],
    (component) => encodeComponent(component, component.extract(value))
  )

const writeEntity = (
  sql: SqlClient.SqlClient,
  descriptor: Descriptor,
  generation: number,
  entityKey: string,
  value: unknown
): Effect.Effect<void, ReplicaError.StorageError> =>
  Effect.gen(function*() {
    const table = sql(descriptor.tableName)
    yield* sql`DELETE FROM ${table}
      WHERE index_generation = ${generation} AND index_entity_key = ${entityKey}`
    if (value === undefined) return
    const values = yield* encodedComponents(descriptor, value)
    const row: Record<string, unknown> = {
      index_generation: generation,
      index_entity_key: entityKey
    }
    for (let position = 0; position < descriptor.index.partition.length; position++) {
      row[`p${position}`] = values[position]
    }
    for (let position = 0; position < descriptor.index.sort.length; position++) {
      row[`s${position}`] = values[descriptor.index.partition.length + position]
    }
    yield* sql`INSERT INTO ${table} ${sql.insert(row)}`
  }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

const decodeEntity = (model: Model.Any, valueJson: string) =>
  Codec.parse(valueJson).pipe(Effect.flatMap((encoded) => Codec.decode(model.schema, encoded)))

const ensureCatalog = (
  sql: SqlClient.SqlClient,
  descriptor: Descriptor
): Effect.Effect<typeof CatalogRow.Type, ReplicaError.StorageError> => {
  const findCatalog = SqlSchema.findOneOption({
    Request: Schema.Struct({ model: Schema.String, indexName: Schema.String, hash: Schema.String }),
    Result: CatalogRow,
    execute: ({ hash, indexName, model }) =>
      sql`SELECT layout_hash, table_name, table_checksum, scan_index_name, scan_index_checksum,
        backfill_generation, backfill_after_key, ready_generation
      FROM effect_local_client_index_catalog
      WHERE model = ${model} AND index_name = ${indexName} AND descriptor_hash = ${hash}`
  })
  return Effect.gen(function*() {
    yield* sql.unsafe(descriptor.tableDdl)
    yield* sql.unsafe(descriptor.scanIndexDdl)
    const stored = yield* findCatalog({
      model: descriptor.model.name,
      indexName: descriptor.indexName,
      hash: descriptor.hash
    }).pipe(Effect.mapError(StorageUnavailable.make))
    if (Option.isNone(stored)) {
      yield* sql`INSERT INTO effect_local_client_index_catalog
        (model, index_name, descriptor_hash, layout_hash, table_name, table_checksum,
          scan_index_name, scan_index_checksum)
        VALUES (${descriptor.model.name}, ${descriptor.indexName}, ${descriptor.hash}, ${descriptor.hash},
          ${descriptor.tableName}, ${descriptor.tableChecksum}, ${descriptor.scanIndexName},
          ${descriptor.scanIndexChecksum})`
      return {
        layout_hash: descriptor.hash,
        table_name: descriptor.tableName,
        table_checksum: descriptor.tableChecksum,
        scan_index_name: descriptor.scanIndexName,
        scan_index_checksum: descriptor.scanIndexChecksum,
        backfill_generation: null,
        backfill_after_key: null,
        ready_generation: null
      }
    }
    const row = stored.value
    if (
      row.layout_hash !== descriptor.hash || row.table_name !== descriptor.tableName ||
      row.table_checksum !== descriptor.tableChecksum || row.scan_index_name !== descriptor.scanIndexName ||
      row.scan_index_checksum !== descriptor.scanIndexChecksum
    ) {
      return yield* storageCorrupt(
        `Secondary index catalog checksum mismatch for ${descriptor.model.name}.${descriptor.indexName}`
      )
    }
    return row
  }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
}

const findGeneration = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result: GenerationRow,
    execute: () => sql`SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1`
  })(undefined).pipe(Effect.mapError(StorageUnavailable.make))

const backfill = (
  sql: SqlClient.SqlClient,
  descriptor: Descriptor,
  initial: typeof CatalogRow.Type
): Effect.Effect<void, ReplicaError.StorageError> =>
  Effect.gen(function*() {
    const generation = (yield* findGeneration(sql)).active_schema_generation
    if (initial.ready_generation === generation) return
    let after = initial.backfill_after_key
    if (initial.backfill_generation !== generation) after = null
    if (initial.backfill_generation !== generation) {
      yield* sql.withTransaction(Effect.gen(function*() {
        const table = sql(descriptor.tableName)
        yield* sql`DELETE FROM ${table} WHERE index_generation = ${generation}`
        yield* sql`UPDATE effect_local_client_index_catalog
          SET backfill_generation = ${generation}, backfill_after_key = NULL, ready_generation = NULL
          WHERE model = ${descriptor.model.name} AND index_name = ${descriptor.indexName}
            AND descriptor_hash = ${descriptor.hash}`
      }))
    }
    const findPage = SqlSchema.findAll({
      Request: Schema.Struct({ generation: Schema.Int, model: Schema.String, after: Schema.NullOr(Schema.String) }),
      Result: IndexedEntityRow,
      execute: ({ after: pageAfter, generation: pageGeneration, model: pageModel }) => {
        if (pageAfter === null) {
          return sql`SELECT entity_key, value_json FROM effect_local_client_visible_entities_data
            WHERE generation = ${pageGeneration} AND model = ${pageModel}
            ORDER BY entity_key LIMIT ${backfillPageSize}`
        }
        return sql`SELECT entity_key, value_json FROM effect_local_client_visible_entities_data
          WHERE generation = ${pageGeneration} AND model = ${pageModel} AND entity_key > ${pageAfter}
          ORDER BY entity_key LIMIT ${backfillPageSize}`
      }
    })
    while (true) {
      const current = yield* findGeneration(sql)
      if (current.active_schema_generation !== generation) {
        yield* storageCorrupt("Schema generation changed while secondary indexes were backfilling")
      }
      const rows = yield* findPage({ generation, model: descriptor.model.name, after }).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      if (rows.length === 0) {
        yield* sql`UPDATE effect_local_client_index_catalog
          SET backfill_generation = NULL, backfill_after_key = NULL, ready_generation = ${generation}
          WHERE model = ${descriptor.model.name} AND index_name = ${descriptor.indexName}
            AND descriptor_hash = ${descriptor.hash}`
        return
      }
      yield* sql.withTransaction(Effect.gen(function*() {
        for (const row of rows) {
          const value = yield* decodeEntity(descriptor.model, row.value_json)
          yield* writeEntity(sql, descriptor, generation, row.entity_key, value)
        }
        after = rows[rows.length - 1].entity_key
        yield* sql`UPDATE effect_local_client_index_catalog SET backfill_after_key = ${after}
          WHERE model = ${descriptor.model.name} AND index_name = ${descriptor.indexName}
            AND descriptor_hash = ${descriptor.hash} AND backfill_generation = ${generation}`
      }))
    }
  }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

export interface Runtime {
  readonly update: (
    model: Model.Any,
    generation: number,
    entityKey: string,
    value: unknown
  ) => Effect.Effect<void, ReplicaError.StorageError>
  readonly rebuild: (generation: number) => Effect.Effect<void, ReplicaError.StorageError>
}

export const install = (
  sql: SqlClient.SqlClient,
  definition: Definition.Any
): Effect.Effect<Runtime, ReplicaError.StorageError> =>
  Effect.gen(function*() {
    const all = descriptors(definition)
    for (const descriptor of all) {
      const catalog = yield* ensureCatalog(sql, descriptor)
      yield* backfill(sql, descriptor, catalog)
    }
    const byModel = new Map<string, ReadonlyArray<Descriptor>>()
    for (const model of definition.models) {
      byModel.set(model.name, all.filter((descriptor) => descriptor.model === model))
    }
    const update: Runtime["update"] = (model, generation, entityKey, value) =>
      Effect.forEach(
        byModel.get(model.name) ?? [],
        (descriptor) => writeEntity(sql, descriptor, generation, entityKey, value),
        { discard: true }
      )
    const rebuild: Runtime["rebuild"] = (generation) =>
      Effect.gen(function*() {
        for (const descriptor of all) {
          const table = sql(descriptor.tableName)
          yield* sql`DELETE FROM ${table} WHERE index_generation = ${generation}`
          const findRows = SqlSchema.findAll({
            Request: Schema.Struct({ generation: Schema.Int, model: Schema.String }),
            Result: IndexedEntityRow,
            execute: ({ generation: rowGeneration, model: rowModel }) =>
              sql`SELECT entity_key, value_json FROM effect_local_client_visible_entities_data
              WHERE generation = ${rowGeneration} AND model = ${rowModel} ORDER BY entity_key`
          })
          const rows = yield* findRows({ generation, model: descriptor.model.name }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          for (const row of rows) {
            const value = yield* decodeEntity(descriptor.model, row.value_json)
            yield* writeEntity(sql, descriptor, generation, row.entity_key, value)
          }
          yield* sql`UPDATE effect_local_client_index_catalog SET ready_generation = ${generation},
            backfill_generation = NULL, backfill_after_key = NULL
            WHERE model = ${descriptor.model.name} AND index_name = ${descriptor.indexName}
              AND descriptor_hash = ${descriptor.hash}`
        }
      }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
    return { update, rebuild }
  })

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
        return yield* storageCorrupt(`Query is missing partition ${component.name}`)
      }
      const encoded = yield* encodeComponent(component, value)
      shape[component.name] = encoded
    }
    const firstSort = descriptor.index.sort[0]
    if (firstSort !== undefined && where !== undefined) {
      const bounds = Reflect.get(where, firstSort.name)
      if (bounds !== undefined) {
        if (bounds === null || typeof bounds !== "object") {
          return yield* storageCorrupt(`Bounds for ${firstSort.name} must be an object`)
        }
        const encodedBounds: Record<string, SqlValue> = {}
        for (const operator of ["gt", "gte", "lt", "lte"] as const) {
          const value = Reflect.get(bounds, operator)
          if (value !== undefined) encodedBounds[operator] = yield* encodeComponent(firstSort, value)
        }
        if (encodedBounds.gt !== undefined && encodedBounds.gte !== undefined) {
          return yield* storageCorrupt(`Bounds for ${firstSort.name} cannot contain both gt and gte`)
        }
        if (encodedBounds.lt !== undefined && encodedBounds.lte !== undefined) {
          return yield* storageCorrupt(`Bounds for ${firstSort.name} cannot contain both lt and lte`)
        }
        shape[firstSort.name] = encodedBounds
      }
    }
    return { shape }
  })

const runPage = <M extends Model.Any, Name extends keyof Model.Indexes<M> & string,>(
  sql: SqlClient.SqlClient,
  generation: number,
  model: M,
  indexName: Name,
  index: SecondaryIndex.Any,
  state: QueryState
): Effect.Effect<
  SecondaryIndex.Page<Model.Value<M>, SecondaryIndex.Cursor<M["name"], Name>>,
  ReplicaError.StorageError
> => {
  const descriptor = makeDescriptor(model, indexName, index)
  return Effect.gen(function*() {
    if (!Number.isSafeInteger(state.limit) || state.limit <= 0 || state.limit > 1_000) {
      return yield* storageCorrupt("Query limit must be a positive safe integer no greater than 1000")
    }
    const ready = yield* SqlSchema.findOneOption({
      Request: Schema.Struct({ model: Schema.String, indexName: Schema.String, hash: Schema.String }),
      Result: ReadyRow,
      execute: ({ hash, indexName: readyIndexName, model: readyModel }) =>
        sql`SELECT layout_hash, ready_generation FROM effect_local_client_index_catalog
        WHERE model = ${readyModel} AND index_name = ${readyIndexName} AND descriptor_hash = ${hash}`
    })({ model: model.name, indexName, hash: descriptor.hash }).pipe(Effect.mapError(StorageUnavailable.make))
    if (
      Option.isNone(ready) || ready.value.layout_hash !== descriptor.hash ||
      ready.value.ready_generation !== generation
    ) {
      return yield* storageCorrupt(`Secondary index ${model.name}.${indexName} is not ready for this generation`)
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
      ...descriptor.index.sort.map((_, position) => `s${position}`),
      "index_entity_key"
    ]
    if (state.cursor !== undefined) {
      const cursor = yield* Schema.decodeUnknownEffect(CursorFromString)(state.cursor).pipe(
        Effect.mapError((cause) => storageCorrupt("Query cursor is invalid", cause))
      )
      if (cursor.shape !== shape || cursor.values.length !== orderedColumns.length) {
        return yield* storageCorrupt("Query cursor does not belong to this query shape")
      }
      const comparisons = orderedColumns.map((name, position) => {
        const equals = orderedColumns.slice(0, position).map((prefix, prefixPosition) =>
          sql`${column(sql, prefix)} = ${cursor.values[prefixPosition]}`
        )
        let token = "<"
        if (state.direction === "asc") token = ">"
        return sql.and([
          ...equals,
          sql`${column(sql, name)} ${sql.literal(token)} ${cursor.values[position]}`
        ])
      })
      whereClauses.push(sql.or(comparisons))
    }
    const table = sql(descriptor.tableName)
    const order = sql.csv(
      orderedColumns.map((name) => sql`${column(sql, name)} ${sql.literal(state.direction.toUpperCase())}`)
    )
    const findRows = SqlSchema.findAll({
      Request: Schema.Void,
      Result: IndexedEntityRow,
      execute: () =>
        sql`SELECT e.entity_key, e.value_json FROM ${table} AS i
        INNER JOIN effect_local_client_visible_entities_data AS e
          ON e.generation = i.index_generation AND e.model = ${model.name}
          AND e.entity_key = i.index_entity_key
        WHERE i.index_generation = ${generation} AND ${sql.and(whereClauses)}
        ORDER BY ${order} LIMIT ${state.limit + 1}`
    })
    const rows = yield* findRows(undefined).pipe(Effect.mapError(StorageUnavailable.make))
    const pageRows = rows.slice(0, state.limit)
    const items = yield* Effect.forEach(pageRows, (row) => decodeEntity(model, row.value_json))
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
        Effect.mapError((cause) => storageCorrupt("Query cursor could not be encoded", cause))
      )
      next = { model: model.name, index: indexName, token }
    }
    return { items, next }
  }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
}

export const query = <M extends Model.Any, Name extends keyof Model.Indexes<M> & string,>(
  sql: SqlClient.SqlClient,
  generation: number,
  model: M,
  indexName: Name
): SecondaryIndex.Builder<M["name"], Name, Model.Value<M>, Model.Indexes<M>[Name]> => {
  const index = model.indexes[indexName]
  const build = (
    state: QueryState
  ): SecondaryIndex.Builder<M["name"], Name, Model.Value<M>, Model.Indexes<M>[Name]> => ({
    where: (where) => build({ ...state, where }),
    order: (direction) => build({ ...state, direction }),
    limit: (limit) => build({ ...state, limit }),
    after: (cursor) => build({ ...state, cursor: cursor.token }),
    page: () => runPage(sql, generation, model, indexName, index, state),
    stream: () =>
      Stream.paginate(
        state.cursor,
        (cursor) =>
          runPage(sql, generation, model, indexName, index, { ...state, cursor }).pipe(
            Effect.map((page) => {
              if (page.next === undefined) return [page.items, Option.none<string>()] as const
              return [page.items, Option.some(page.next.token)] as const
            })
          )
      )
  })
  return build({ where: undefined, direction: "asc", limit: 50, cursor: undefined })
}
