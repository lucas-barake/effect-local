import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as SchemaDescriptor from "@lucas-barake/effect-local/SchemaDescriptor"
import type * as SecondaryIndex from "@lucas-barake/effect-local/SecondaryIndex"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import type * as Statement from "effect/unstable/sql/Statement"
import * as Codec from "./codec.js"
import { affinitySql, encodedComponents, encodedPrimitive, type SqlValue } from "./indexComponents.js"
import * as StorageUnavailable from "./storageUnavailable.js"

interface Descriptor {
  readonly model: Model.Any
  readonly indexName: string
  readonly index: SecondaryIndex.Any
  readonly hash: string
  readonly tableName: string
  readonly scanIndexName: string
  readonly tableDdl: string
  readonly scanIndexDdl: string
}

export interface Runtime {
  readonly apply: (
    spaceId: Identity.SpaceId,
    schemaGeneration: number,
    serverSequence: number,
    changes: ReadonlyArray<Protocol.EntityChange>
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly membership: (
    spaceId: Identity.SpaceId,
    schemaGeneration: number,
    window: Protocol.ReplicationWindow
  ) => Effect.Effect<ReadonlySet<string>, ReplicaError.ReplicaError>
  readonly partitionMembership: (
    spaceId: Identity.SpaceId,
    schemaGeneration: number,
    window: Protocol.ReplicationWindow,
    partitions: ReadonlyArray<ReadonlyArray<SqlValue>>
  ) => Effect.Effect<ReadonlySet<string>, ReplicaError.ReplicaError>
  readonly partitionsOf: (
    spaceId: Identity.SpaceId,
    schemaGeneration: number,
    window: Protocol.ReplicationWindow,
    entityKeys: ReadonlyArray<string>
  ) => Effect.Effect<ReadonlyMap<string, ReadonlyArray<SqlValue>>, ReplicaError.ReplicaError>
  readonly affectedPartitions: (
    spaceId: Identity.SpaceId,
    schemaGeneration: number,
    window: Protocol.ReplicationWindow,
    afterSequence: number
  ) => Effect.Effect<ReadonlyArray<ReadonlyArray<SqlValue>>, ReplicaError.ReplicaError>
}

const EntityKeyRow = Schema.Struct({ entity_key: Schema.String })

const StateRow = Schema.Struct({ built: Schema.Literals([0, 1]) })
const DescriptorHash = Schema.String.check(Schema.isPattern(/^[0-9a-f]{16}$/))

const BuiltDescriptorRow = Schema.Struct({ descriptor_hash: DescriptorHash })

const CatalogRow = Schema.Struct({
  model: Schema.String,
  index_name: Schema.String,
  descriptor_hash: DescriptorHash,
  table_name: Schema.String,
  scan_index_name: Schema.String
})

const BackfillRow = Schema.Struct({
  entity_key: Schema.String,
  value_json: Schema.String
})
const PartitionValues = Schema.Array(Schema.Union([Schema.String, Schema.Number]))

const backfillPageSize = 500

const makeDescriptor = (model: Model.Any, indexName: string, index: SecondaryIndex.Any): Descriptor => {
  const hash = Canonical.hash({
    format: 3,
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
  const tableName = `effect_local_srvidx_${hash}`
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
    space_id TEXT NOT NULL,
    schema_generation INTEGER NOT NULL CHECK (schema_generation >= 0),
    entity_key TEXT NOT NULL,
    ${componentColumns.join(",\n    ")},
    PRIMARY KEY (space_id, schema_generation, entity_key)
  ) WITHOUT ROWID`
  const scanColumns = [
    "space_id",
    "schema_generation",
    ...index.partition.map((_, position) => `p${position}`),
    ...index.sort.map((_, position) => `s${position} DESC`),
    "entity_key DESC"
  ]
  const scanIndexDdl = `CREATE INDEX IF NOT EXISTS ${scanIndexName} ON ${tableName} (${scanColumns.join(", ")})`
  return { model, indexName, index, hash, tableName, scanIndexName, tableDdl, scanIndexDdl }
}

const partitionColumns = (descriptor: Descriptor): ReadonlyArray<string> =>
  descriptor.index.partition.map((_, position) => `p${position}`)

const sortColumns = (descriptor: Descriptor): ReadonlyArray<string> =>
  descriptor.index.sort.map((_, position) => `s${position}`)

const indexRow = (
  descriptor: Descriptor,
  spaceId: Identity.SpaceId,
  schemaGeneration: number,
  entityKey: string,
  values: ReadonlyArray<SqlValue>
): Record<string, unknown> => {
  const row: Record<string, unknown> = {
    space_id: spaceId,
    schema_generation: schemaGeneration,
    entity_key: entityKey
  }
  for (let position = 0; position < descriptor.index.partition.length; position++) {
    row[`p${position}`] = values[position]
  }
  for (let position = 0; position < descriptor.index.sort.length; position++) {
    row[`s${position}`] = values[descriptor.index.partition.length + position]
  }
  return row
}

export const make = Effect.fn("ServerIndex.make")(
  function*(
    sql: SqlClient.SqlClient,
    definition: Definition.Any
  ) {
    const all = definition.models.flatMap((model) =>
      Object.entries(model.indexes).map(([indexName, index]) => makeDescriptor(model, indexName, index))
    )
    const byModel = new Map<string, ReadonlyArray<Descriptor>>()
    for (const model of definition.models) {
      byModel.set(model.name, all.filter((descriptor) => descriptor.model === model))
    }
    const byLabel = new Map(
      all.map((descriptor) => [Canonical.stringify([descriptor.model.name, descriptor.indexName]), descriptor])
    )
    for (const descriptor of all) {
      yield* sql.unsafe(descriptor.tableDdl)
      yield* sql.unsafe(descriptor.scanIndexDdl)
      yield* sql`INSERT INTO effect_local_server_index_catalog
        (model, index_name, descriptor_hash, table_name, scan_index_name)
        VALUES (${descriptor.model.name}, ${descriptor.indexName}, ${descriptor.hash},
          ${descriptor.tableName}, ${descriptor.scanIndexName})
        ON CONFLICT (model, index_name, descriptor_hash) DO NOTHING`
    }
    const catalog = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: CatalogRow,
      execute: () =>
        sql`SELECT model, index_name, descriptor_hash, table_name, scan_index_name
        FROM effect_local_server_index_catalog`
    })(undefined).pipe(
      Effect.catchTag(
        "SchemaError",
        (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server index catalog is invalid", cause }))
      )
    )
    const byHash = new Map(all.map((descriptor) => [descriptor.hash, descriptor]))
    for (const row of catalog) {
      const tableName = `effect_local_srvidx_${row.descriptor_hash}`
      const scanIndexName = `${tableName}_scan`
      const descriptor = byHash.get(row.descriptor_hash)
      if (
        row.table_name !== tableName || row.scan_index_name !== scanIndexName ||
        (descriptor !== undefined &&
          (row.model !== descriptor.model.name || row.index_name !== descriptor.indexName))
      ) {
        return yield* new ReplicaError.StorageCorrupt({
          message: "Server index catalog conflicts with its descriptor metadata"
        })
      }
      if (descriptor !== undefined) {
        continue
      }
      yield* sql.unsafe(`DROP INDEX IF EXISTS ${scanIndexName}`)
      yield* sql.unsafe(`DROP TABLE IF EXISTS ${tableName}`)
      yield* sql`DELETE FROM effect_local_server_index_state WHERE descriptor_hash = ${row.descriptor_hash}`
      yield* sql`DELETE FROM effect_local_server_index_catalog
        WHERE model = ${row.model} AND index_name = ${row.index_name}
          AND descriptor_hash = ${row.descriptor_hash}`
    }

    const findState = SqlSchema.findOneOption({
      Request: Schema.Struct({
        spaceId: Schema.String,
        schemaGeneration: Schema.Int,
        descriptorHash: Schema.String
      }),
      Result: StateRow,
      execute: ({ descriptorHash, schemaGeneration, spaceId }) =>
        sql`SELECT built FROM effect_local_server_index_state
        WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
          AND descriptor_hash = ${descriptorHash}`
    })

    const findBuiltDescriptors = SqlSchema.findAll({
      Request: Schema.Struct({ spaceId: Schema.String, schemaGeneration: Schema.Int }),
      Result: BuiltDescriptorRow,
      execute: ({ schemaGeneration, spaceId }) =>
        sql`SELECT descriptor_hash FROM effect_local_server_index_state
        WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration} AND built = 1`
    })

    const writeRows = (descriptor: Descriptor, rows: ReadonlyArray<Record<string, unknown>>) => {
      if (rows.length === 0) {
        return Effect.void
      }
      const table = sql(descriptor.tableName)
      const columns = [...partitionColumns(descriptor), ...sortColumns(descriptor)]
      const assignmentClauses = columns.map((name) => {
        const column = sql(name)
        return sql`${column} = excluded.${column}`
      })
      const assignments = sql.join(", ", false)(assignmentClauses)
      const batchSize = Math.max(1, Math.floor(900 / (columns.length + 3)))
      const batches = Array.from(
        { length: Math.ceil(rows.length / batchSize) },
        (_, position) => rows.slice(position * batchSize, (position + 1) * batchSize)
      )
      return Effect.forEach(
        batches,
        (batch) => {
          const insert = sql.insert(batch)
          return sql`INSERT INTO ${table} ${insert}
          ON CONFLICT (space_id, schema_generation, entity_key)
          DO UPDATE SET ${assignments}`
        },
        { discard: true }
      )
    }

    const decodeValue = (model: Model.Any, valueJson: string) =>
      Codec.parse(valueJson).pipe(Effect.flatMap((parsed) => Codec.decode(model.schema, parsed)))

    const backfill = Effect.fnUntraced(function*(
      spaceId: Identity.SpaceId,
      schemaGeneration: number,
      descriptor: Descriptor
    ) {
      const table = sql(descriptor.tableName)
      yield* sql`DELETE FROM ${table} WHERE space_id = ${spaceId} AND schema_generation <> ${schemaGeneration}`
      yield* sql`DELETE FROM ${table} WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}`
      let after = ""
      while (true) {
        const rows = yield* SqlSchema.findAll({
          Request: Schema.Struct({ spaceId: Schema.String, after: Schema.String, limit: Schema.Int }),
          Result: BackfillRow,
          execute: ({ after: cursor, limit, spaceId: requestedSpaceId }) =>
            sql`SELECT entity_key, value_json FROM effect_local_server_entities
              WHERE space_id = ${requestedSpaceId} AND model = ${descriptor.model.name}
                AND entity_key > ${cursor}
              ORDER BY entity_key LIMIT ${limit}`
        })({ spaceId, after, limit: backfillPageSize }).pipe(
          Effect.catchTag(
            "SchemaError",
            (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server entity row is invalid", cause }))
          )
        )
        if (rows.length === 0) break
        const encoded: Array<Record<string, unknown>> = []
        for (const row of rows) {
          const value = yield* decodeValue(descriptor.model, row.value_json)
          const values = yield* encodedComponents(descriptor.index, value)
          encoded.push(indexRow(descriptor, spaceId, schemaGeneration, row.entity_key, values))
        }
        yield* writeRows(descriptor, encoded)
        after = rows[rows.length - 1].entity_key
        if (rows.length < backfillPageSize) break
      }
      yield* sql`DELETE FROM effect_local_server_index_state
          WHERE space_id = ${spaceId} AND descriptor_hash = ${descriptor.hash}
            AND schema_generation <> ${schemaGeneration}`
      yield* sql`INSERT INTO effect_local_server_index_state
          (space_id, schema_generation, descriptor_hash, built)
          VALUES (${spaceId}, ${schemaGeneration}, ${descriptor.hash}, 1)
          ON CONFLICT (space_id, schema_generation, descriptor_hash) DO UPDATE SET built = 1`
    })

    const ensureBuilt = Effect.fnUntraced(function*(
      spaceId: Identity.SpaceId,
      schemaGeneration: number,
      descriptor: Descriptor
    ) {
      const state = yield* findState({ spaceId, schemaGeneration, descriptorHash: descriptor.hash }).pipe(
        Effect.catchTag(
          "SchemaError",
          (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server index state is invalid", cause }))
        )
      )
      if (state._tag === "Some" && state.value.built === 1) return
      yield* backfill(spaceId, schemaGeneration, descriptor)
    })

    const readPartitions = Effect.fnUntraced(function*(
      descriptor: Descriptor,
      spaceId: Identity.SpaceId,
      schemaGeneration: number,
      entityKeys: ReadonlyArray<string>
    ) {
      const found = new Map<string, ReadonlyArray<SqlValue>>()
      const partitions = partitionColumns(descriptor)
      for (let offset = 0; offset < entityKeys.length; offset += 100) {
        const batch = entityKeys.slice(offset, offset + 100)
        if (partitions.length === 0) {
          const rows = yield* SqlSchema.findAll({
            Request: Schema.Void,
            Result: EntityKeyRow,
            execute: () =>
              sql`SELECT entity_key FROM ${sql(descriptor.tableName)}
                WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
                  AND entity_key IN ${sql.in(batch)}`
          })(undefined).pipe(
            Effect.catchTag(
              "SchemaError",
              (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server index row is invalid", cause }))
            )
          )
          for (const row of rows) found.set(row.entity_key, [])
          continue
        }
        const partitionIdentifiers = partitions.map((name) => sql.literal(name))
        const valuesJson = sql.csv(partitionIdentifiers)
        const rows = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: Schema.Struct({ entity_key: Schema.String, values_json: Schema.String }),
          execute: () =>
            sql`SELECT entity_key, json_array(${valuesJson}) AS values_json
              FROM ${sql(descriptor.tableName)}
              WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
                AND entity_key IN ${sql.in(batch)}`
        })(undefined).pipe(
          Effect.catchTag(
            "SchemaError",
            (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server index row is invalid", cause }))
          )
        )
        for (const row of rows) {
          found.set(
            row.entity_key,
            yield* Codec.parse(row.values_json).pipe(
              Effect.flatMap((parsed) => Codec.decode(PartitionValues, parsed))
            )
          )
        }
      }
      return found
    })

    const writePartitionLogs = (rows: ReadonlyArray<Record<string, unknown>>) => {
      if (rows.length === 0) return Effect.void
      const batches = Array.from(
        { length: Math.ceil(rows.length / 180) },
        (_, position) => rows.slice(position * 180, (position + 1) * 180)
      )
      return Effect.forEach(
        batches,
        (batch) => {
          const insert = sql.insert(batch)
          return sql`INSERT INTO effect_local_server_index_partition_log ${insert}
          ON CONFLICT (space_id, server_sequence, descriptor_hash, partition_json) DO NOTHING`
        },
        { discard: true }
      )
    }

    const apply: Runtime["apply"] = Effect.fnUntraced(function*(
      spaceId,
      schemaGeneration,
      serverSequence,
      changes
    ) {
      const states = yield* findBuiltDescriptors({ spaceId, schemaGeneration }).pipe(
        Effect.catchTag(
          "SchemaError",
          (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server index state is invalid", cause }))
        )
      )
      const built = new Set(states.map((row) => row.descriptor_hash))
      for (const [modelName, descriptors] of byModel) {
        const active = descriptors.filter((descriptor) => built.has(descriptor.hash))
        if (active.length === 0) continue
        const modelChanges = changes.filter((change) => change.entity.model === modelName)
        if (modelChanges.length === 0) continue
        const prepared: Array<{
          readonly change: Protocol.EntityChange
          readonly entityKey: string
          readonly value: unknown
        }> = []
        const model = definition.modelByName.get(modelName)
        if (model === undefined) continue
        for (const change of modelChanges) {
          let value: unknown
          if (change._tag === "Upsert") {
            value = yield* Codec.decode(model.schema, change.value)
          }
          prepared.push({
            change,
            entityKey: yield* Codec.stringify(change.entity.key),
            value
          })
        }
        for (const descriptor of active) {
          const previousByKey = yield* readPartitions(
            descriptor,
            spaceId,
            schemaGeneration,
            prepared.map((item) => item.entityKey)
          )
          const logs: Array<Record<string, unknown>> = []
          const rows: Array<Record<string, unknown>> = []
          const deleted: Array<string> = []
          const log = (values: ReadonlyArray<SqlValue>) => {
            logs.push({
              space_id: spaceId,
              schema_generation: schemaGeneration,
              server_sequence: serverSequence,
              descriptor_hash: descriptor.hash,
              partition_json: Canonical.stringify(values)
            })
          }
          for (const item of prepared) {
            const previous = previousByKey.get(item.entityKey)
            if (item.change._tag === "Delete") {
              if (previous !== undefined) log(previous)
              deleted.push(item.entityKey)
              continue
            }
            const values = yield* encodedComponents(descriptor.index, item.value)
            const next = values.slice(0, descriptor.index.partition.length)
            if (previous !== undefined && Canonical.stringify(previous) !== Canonical.stringify(next)) log(previous)
            log(next)
            rows.push(indexRow(descriptor, spaceId, schemaGeneration, item.entityKey, values))
          }
          for (let offset = 0; offset < deleted.length; offset += 100) {
            yield* sql`DELETE FROM ${sql(descriptor.tableName)}
                WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
                  AND entity_key IN ${sql.in(deleted.slice(offset, offset + 100))}`
          }
          yield* writePartitionLogs(logs)
          yield* writeRows(descriptor, rows)
        }
      }
    }, Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

    const encodedPartitionKey = (
      descriptor: Descriptor,
      key: ReadonlyArray<Protocol.WindowComponentValue>
    ) =>
      Effect.forEach(
        descriptor.index.partition,
        (component, position) => encodedPrimitive(component, key[position])
      )

    const partitionMatches = (
      descriptor: Descriptor,
      values: ReadonlyArray<SqlValue>
    ): Statement.Fragment => {
      const columns = partitionColumns(descriptor)
      if (columns.length === 0) return sql`1 = 1`
      const clauses = columns.map((name, position) => {
        const column = sql(name)
        return sql`${column} = ${values[position]}`
      })
      return sql.and(clauses)
    }

    const boundsClauses = Effect.fnUntraced(function*(
      descriptor: Descriptor,
      bounds: Protocol.ReplicationWindowBounds
    ) {
      const leading = descriptor.index.sort[0]
      const column = sql(sortColumns(descriptor)[0])
      const clauses: Array<Statement.Fragment> = []
      if (bounds.gt !== undefined) clauses.push(sql`${column} > ${yield* encodedPrimitive(leading, bounds.gt)}`)
      if (bounds.gte !== undefined) clauses.push(sql`${column} >= ${yield* encodedPrimitive(leading, bounds.gte)}`)
      if (bounds.lt !== undefined) clauses.push(sql`${column} < ${yield* encodedPrimitive(leading, bounds.lt)}`)
      if (bounds.lte !== undefined) clauses.push(sql`${column} <= ${yield* encodedPrimitive(leading, bounds.lte)}`)
      return clauses
    })

    const membership: Runtime["membership"] = Effect.fnUntraced(function*(spaceId, schemaGeneration, window) {
      const descriptor = byLabel.get(Canonical.stringify([window.model, window.index]))
      if (descriptor === undefined) {
        return yield* new ReplicaError.ProtocolInvalid({
          message: `Unknown replication window index: ${window.model}/${window.index}`
        })
      }
      yield* ensureBuilt(spaceId, schemaGeneration, descriptor)
      const table = sql(descriptor.tableName)
      const partitions = partitionColumns(descriptor)
      const sorts = sortColumns(descriptor)
      const orderColumns = [...sorts, "entity_key"].map((name) => {
        const column = sql(name)
        return sql`${column} DESC`
      })
      const orderTuple = sql.join(", ", false)(
        orderColumns
      )
      const selected = new Set<string>()
      const overrides = window.partitions ?? []
      const overrideValues: Array<ReadonlyArray<SqlValue>> = []
      for (const override of overrides) {
        overrideValues.push(yield* encodedPartitionKey(descriptor, override.key))
      }
      const collect = (statement: Statement.Statement<any>) =>
        SqlSchema.findAll({
          Request: Schema.Void,
          Result: EntityKeyRow,
          execute: () => statement
        })(undefined).pipe(
          Effect.catchTag(
            "SchemaError",
            (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server index row is invalid", cause }))
          ),
          Effect.map((rows) => {
            for (const row of rows) selected.add(row.entity_key)
          })
        )
      let exclusion = sql`1 = 1`
      if (overrideValues.length > 0) {
        const overrideClauses = partitions.map((name, position) => {
          const column = sql(name)
          return sql`${column} = json_extract(override.value, ${`$[${position}]`})`
        })
        const overrideMatch = sql.and(overrideClauses)
        exclusion = sql`NOT EXISTS (
            SELECT 1 FROM json_each(${Canonical.stringify(overrideValues)}) AS override
            WHERE ${overrideMatch}
          )`
      }
      if (partitions.length === 0) {
        if (overrideValues.length === 0) {
          yield* collect(
            sql`SELECT entity_key FROM ${table}
              WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
              ORDER BY ${orderTuple} LIMIT ${window.count}`
          )
        }
      } else {
        const partitionIdentifiers = partitions.map((name) => {
          const column = sql(name)
          return sql`${column}`
        })
        const partitionTuple = sql.join(", ", false)(partitionIdentifiers)
        yield* collect(
          sql`SELECT entity_key FROM (
              SELECT entity_key, ${partitionTuple},
                ROW_NUMBER() OVER (PARTITION BY ${partitionTuple} ORDER BY ${orderTuple}) AS window_rank
              FROM ${table}
              WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration} AND ${exclusion}
            ) WHERE window_rank <= ${window.count}`
        )
      }
      for (let position = 0; position < overrides.length; position++) {
        const override = overrides[position]
        const matches = partitionMatches(descriptor, overrideValues[position])
        const count = override.count ?? window.count
        yield* collect(
          sql`SELECT entity_key FROM ${table}
            WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration} AND ${matches}
            ORDER BY ${orderTuple} LIMIT ${count}`
        )
        if (override.bounds !== undefined) {
          const clauses = yield* boundsClauses(descriptor, override.bounds)
          if (clauses.length > 0) {
            yield* collect(
              sql`SELECT entity_key FROM ${table}
                WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration} AND ${matches}
                  AND ${sql.and(clauses)}`
            )
          }
        }
      }
      return selected
    }, Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

    const resolveDescriptor = (window: Protocol.ReplicationWindow) => {
      const descriptor = byLabel.get(Canonical.stringify([window.model, window.index]))
      if (descriptor === undefined) {
        return Effect.fail(
          new ReplicaError.ProtocolInvalid({
            message: `Unknown replication window index: ${window.model}/${window.index}`
          })
        )
      }
      return Effect.succeed(descriptor)
    }

    const overrideByPartition = Effect.fnUntraced(function*(
      descriptor: Descriptor,
      window: Protocol.ReplicationWindow
    ) {
      const overrides = new Map<string, Protocol.ReplicationWindowPartition>()
      for (const override of window.partitions ?? []) {
        const values = yield* encodedPartitionKey(descriptor, override.key)
        overrides.set(Canonical.stringify(values), override)
      }
      return overrides
    })

    const partitionMembership: Runtime["partitionMembership"] = Effect.fnUntraced(function*(
      spaceId,
      schemaGeneration,
      window,
      partitions
    ) {
      const descriptor = yield* resolveDescriptor(window)
      yield* ensureBuilt(spaceId, schemaGeneration, descriptor)
      const table = sql(descriptor.tableName)
      const sorts = sortColumns(descriptor)
      const orderColumns = [...sorts, "entity_key"].map((name) => {
        const column = sql(name)
        return sql`${column} DESC`
      })
      const orderTuple = sql.join(", ", false)(orderColumns)
      const overrides = yield* overrideByPartition(descriptor, window)
      const selected = new Set<string>()
      const visited = new Set<string>()
      const collect = (statement: Statement.Statement<any>) =>
        SqlSchema.findAll({
          Request: Schema.Void,
          Result: EntityKeyRow,
          execute: () => statement
        })(undefined).pipe(
          Effect.catchTag(
            "SchemaError",
            (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server index row is invalid", cause }))
          ),
          Effect.map((rows) => {
            for (const row of rows) selected.add(row.entity_key)
          })
        )
      for (const partition of partitions) {
        const label = Canonical.stringify(partition)
        if (visited.has(label)) continue
        visited.add(label)
        const override = overrides.get(label)
        const matches = partitionMatches(descriptor, partition)
        const count = override?.count ?? window.count
        yield* collect(
          sql`SELECT entity_key FROM ${table}
            WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration} AND ${matches}
            ORDER BY ${orderTuple} LIMIT ${count}`
        )
        if (override?.bounds !== undefined) {
          const clauses = yield* boundsClauses(descriptor, override.bounds)
          if (clauses.length > 0) {
            yield* collect(
              sql`SELECT entity_key FROM ${table}
                WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration} AND ${matches}
                  AND ${sql.and(clauses)}`
            )
          }
        }
      }
      return selected
    }, Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

    const partitionsOf: Runtime["partitionsOf"] = Effect.fnUntraced(function*(
      spaceId,
      schemaGeneration,
      window,
      entityKeys
    ) {
      const descriptor = yield* resolveDescriptor(window)
      yield* ensureBuilt(spaceId, schemaGeneration, descriptor)
      const table = sql(descriptor.tableName)
      const partitions = partitionColumns(descriptor)
      const found = new Map<string, ReadonlyArray<SqlValue>>()
      if (partitions.length === 0) {
        for (const entityKey of entityKeys) found.set(entityKey, [])
        return found
      }
      const RowSchema = Schema.Struct({
        entity_key: Schema.String,
        values_json: Schema.String
      })
      for (let offset = 0; offset < entityKeys.length; offset += 100) {
        const batch = entityKeys.slice(offset, offset + 100)
        const partitionIdentifiers = partitions.map((name) => sql.literal(name))
        const valuesJson = sql.csv(partitionIdentifiers)
        const rows = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: RowSchema,
          execute: () =>
            sql`SELECT entity_key, json_array(${valuesJson}) AS values_json FROM ${table}
              WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
                AND entity_key IN ${sql.in(batch)}`
        })(undefined).pipe(
          Effect.catchTag(
            "SchemaError",
            (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server index row is invalid", cause }))
          )
        )
        for (const row of rows) {
          const values = yield* Codec.parse(row.values_json).pipe(
            Effect.flatMap((parsed) => Codec.decode(PartitionValues, parsed))
          )
          found.set(row.entity_key, values)
        }
      }
      return found
    }, Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

    const affectedPartitions: Runtime["affectedPartitions"] = Effect.fnUntraced(function*(
      spaceId,
      schemaGeneration,
      window,
      afterSequence
    ) {
      const descriptor = yield* resolveDescriptor(window)
      const rows = yield* SqlSchema.findAll({
        Request: Schema.Void,
        Result: Schema.Struct({ partition_json: Schema.String }),
        execute: () =>
          sql`SELECT DISTINCT partition_json FROM effect_local_server_index_partition_log
            WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
              AND descriptor_hash = ${descriptor.hash} AND server_sequence > ${afterSequence}`
      })(undefined).pipe(
        Effect.catchTag(
          "SchemaError",
          (cause) =>
            Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server index partition log is invalid", cause }))
        )
      )
      const partitions: Array<ReadonlyArray<SqlValue>> = []
      for (const row of rows) {
        partitions.push(
          yield* Codec.parse(row.partition_json).pipe(
            Effect.flatMap((parsed) => Codec.decode(PartitionValues, parsed))
          )
        )
      }
      return partitions
    }, Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

    return { apply, membership, partitionMembership, partitionsOf, affectedPartitions }
  },
  Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
)
