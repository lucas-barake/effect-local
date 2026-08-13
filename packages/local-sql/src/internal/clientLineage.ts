import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Codec from "./codec.js"
import * as StorageUnavailable from "./storageUnavailable.js"

const LineageRow = Schema.Struct({ lineage_id: Schema.String })

export const make = (sql: SqlClient.SqlClient, spaceId: Identity.SpaceId) => {
  const readGroup = SqlSchema.findOneOption({
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
      WHERE space_id = ${spaceId} AND source_schema_version = ${request.schemaVersion}
        AND source_schema_hash = ${request.schemaHash}
        AND source_model = ${request.model} AND source_model_version = ${request.modelVersion}
        AND source_key = ${request.key}`
  })
  const readTarget = SqlSchema.findOneOption({
    Request: Schema.Struct({ model: Schema.String, modelVersion: Identity.SchemaVersion, key: Schema.String }),
    Result: LineageRow,
    execute: (request) =>
      sql`SELECT lineage_id FROM effect_local_client_key_lineage_targets
      WHERE space_id = ${spaceId} AND target_model = ${request.model}
        AND target_model_version = ${request.modelVersion}
        AND target_key = ${request.key}`
  })

  return Effect.fnUntraced(function*(model: string, migrated: Evolution.MigratedModel) {
    const aliases = yield* Effect.forEach(
      migrated.aliases,
      (alias) => Codec.stringify(alias.key).pipe(Effect.map((key) => ({ ...alias, key })))
    )
    const groups = new Set<string>()
    let sourceAliases = aliases
    if (aliases.length !== 1) sourceAliases = aliases.slice(0, -1)
    for (const alias of sourceAliases) {
      const found = yield* readGroup({
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
    const target = yield* readTarget({ model, modelVersion: migrated.modelVersion, key: targetKey }).pipe(
      Effect.mapError(StorageUnavailable.make)
    )
    if (Option.isSome(target) && target.value.lineage_id !== lineageId) {
      return yield* new ReplicaError.SchemaKeyCollision({ model, key: targetKey })
    }
    for (const alias of aliases) {
      yield* sql`INSERT INTO effect_local_client_key_lineage_groups
          (space_id, source_schema_version, source_schema_hash, source_model, source_model_version, source_key,
            lineage_id)
          VALUES (${spaceId}, ${alias.schemaIdentity.version}, ${alias.schemaIdentity.hash}, ${model},
            ${alias.modelVersion}, ${alias.key}, ${lineageId})
          ON CONFLICT (space_id, source_schema_version, source_schema_hash, source_model, source_model_version,
            source_key)
          DO NOTHING`
      yield* sql`INSERT INTO effect_local_client_key_lineage
          (space_id, source_schema_version, source_schema_hash, source_model, source_model_version, source_key,
            target_model, target_model_version, target_key)
          VALUES (${spaceId}, ${alias.schemaIdentity.version}, ${alias.schemaIdentity.hash}, ${model},
            ${alias.modelVersion}, ${alias.key}, ${model}, ${migrated.modelVersion}, ${targetKey})
          ON CONFLICT (space_id, source_schema_version, source_schema_hash, source_model, source_model_version,
            source_key)
          DO UPDATE SET target_model = excluded.target_model,
            target_model_version = excluded.target_model_version, target_key = excluded.target_key`
    }
    yield* sql`INSERT INTO effect_local_client_key_lineage_targets
        (space_id, target_model, target_model_version, target_key, lineage_id)
        VALUES (${spaceId}, ${model}, ${migrated.modelVersion}, ${targetKey}, ${lineageId})
        ON CONFLICT (space_id, target_model, target_model_version, target_key) DO NOTHING`
    const storedTarget = yield* readTarget({
      model,
      modelVersion: migrated.modelVersion,
      key: targetKey
    }).pipe(Effect.mapError(StorageUnavailable.make))
    if (Option.isNone(storedTarget) || storedTarget.value.lineage_id !== lineageId) {
      return yield* new ReplicaError.SchemaKeyCollision({ model, key: targetKey })
    }
    return undefined
  })
}
