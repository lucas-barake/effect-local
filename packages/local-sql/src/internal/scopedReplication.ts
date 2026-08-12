import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Codec from "./codec.js"
import * as Rows from "./rows.js"
import * as StorageUnavailable from "./storageUnavailable.js"

export interface Authorization {
  readonly scope: (
    request: Protocol.PullRequest | Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<void, ReplicaError.AuthorizationDenied>
  readonly entity: (
    request: Protocol.PullRequest | Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type,
    entity: Protocol.EntityKey,
    value: typeof Schema.Json.Type
  ) => Effect.Effect<boolean>
}

export interface Options {
  readonly sql: SqlClient.SqlClient
  readonly crypto: Crypto.Crypto
  readonly definition: Definition.Any
  readonly maximumSnapshotEntities: number
  readonly maximumSnapshotBytes: number
  readonly maximumBootstrapPageBytes: number
  readonly authorization: Authorization
  readonly resolveDefinition: (
    schema: Identity.SchemaIdentity
  ) => Effect.Effect<Definition.Any, ReplicaError.ReplicaError>
  readonly projectEntity: (
    target: Definition.Any,
    entity: Protocol.EntityKey,
    value: typeof Schema.Json.Type
  ) => Effect.Effect<
    Option.Option<{ readonly entity: Protocol.EntityKey; readonly value: typeof Schema.Json.Type }>,
    ReplicaError.ReplicaError
  >
}

interface MaterializedEntity {
  readonly identity: string
  readonly entityKey: string
  readonly entity: Protocol.EntityKey
  readonly value: typeof Schema.Json.Type
  readonly valueJson: string
  readonly sourceEntity: Protocol.EntityKey
  readonly sourceValue: typeof Schema.Json.Type
  readonly sourceEntityKey: string
  readonly sourceValueJson: string
}

interface StoredSnapshotEntry {
  readonly entry: Protocol.SnapshotEntry
  readonly sourceModel: string
  readonly sourceIdentity: string
  readonly sourceEntityKey: string
  readonly sourceValueJson: string
}

const identityOf = (model: string, entityKey: string) => `${model}\u0000${entityKey}`

export const make = (options: Options) => {
  const { sql } = options
  const findSpace = SqlSchema.findOne({
    Request: Identity.SpaceId,
    Result: Rows.ReplicationSpaceRow,
    execute: (spaceId) =>
      sql`SELECT definition_hash, schema_version, schema_hash, schema_generation, active_schema_generation,
        target_schema_version, target_schema_hash, migration_hash, next_server_sequence, next_terminal_sequence
      FROM effect_local_server_spaces WHERE space_id = ${spaceId}`
  })
  const findEntities = SqlSchema.findAll({
    Request: Schema.Struct({ spaceId: Identity.SpaceId, limit: Schema.Int }),
    Result: Rows.ServerEntityRow,
    execute: ({ spaceId, limit }) =>
      sql`SELECT model, model_version, entity_key, value_json, entity_bytes
      FROM effect_local_server_entities WHERE space_id = ${spaceId}
      ORDER BY model, entity_key LIMIT ${limit}`
  })
  const findEntitiesByIdentity = SqlSchema.findAll({
    Request: Schema.Struct({ spaceId: Identity.SpaceId, entitiesJson: Schema.String }),
    Result: Rows.ServerEntityRow,
    execute: ({ spaceId, entitiesJson }) =>
      sql`SELECT entity.model, entity.model_version, entity.entity_key, entity.value_json, entity.entity_bytes
      FROM effect_local_server_entities AS entity
      WHERE entity.space_id = ${spaceId} AND EXISTS(
        SELECT 1 FROM json_each(${entitiesJson}) AS requested
        WHERE entity.model = json_extract(requested.value, '$.model')
          AND entity.entity_key = json_extract(requested.value, '$.key')
      ) ORDER BY entity.model, entity.entity_key`
  })
  const findView = SqlSchema.findOneOption({
    Request: Schema.Struct({ spaceId: Identity.SpaceId, clientId: Identity.ClientId }),
    Result: Rows.ReplicationViewRow,
    execute: ({ spaceId, clientId }) =>
      sql`SELECT space_id, client_id, principal_digest, view_id, view_revision, scope_generation,
        scope_json, scope_digest, definition_hash, schema_version, schema_hash, server_sequence
      FROM effect_local_server_replication_views
      WHERE space_id = ${spaceId} AND client_id = ${clientId}`
  })
  const findViewEntities = SqlSchema.findAll({
    Request: Schema.Struct({
      spaceId: Identity.SpaceId,
      clientId: Identity.ClientId,
      viewId: Identity.ReplicationViewId
    }),
    Result: Rows.ReplicationViewEntityRow,
    execute: ({ spaceId, clientId, viewId }) =>
      sql`SELECT model, model_version, entity_key, disposition, value_json
      FROM effect_local_server_replication_view_entities
      WHERE space_id = ${spaceId} AND client_id = ${clientId} AND view_id = ${viewId}
      ORDER BY model, entity_key`
  })
  const findPage = SqlSchema.findOneOption({
    Request: Schema.Struct({ spaceId: Identity.SpaceId, clientId: Identity.ClientId }),
    Result: Rows.ReplicationPageRow,
    execute: ({ spaceId, clientId }) =>
      sql`SELECT principal_digest, view_id, base_revision, target_revision, scope_generation,
        scope_json, scope_digest, server_sequence, changes_json, content_bytes, digest, has_more
      FROM effect_local_server_replication_pages
      WHERE space_id = ${spaceId} AND client_id = ${clientId}`
  })
  const findSnapshot = SqlSchema.findOneOption({
    Request: Identity.SnapshotId,
    Result: Rows.ScopedSnapshotManifestRow,
    execute: (snapshotId) =>
      sql`SELECT snapshot_id, space_id, client_id, principal_digest, definition_hash,
        schema_version, schema_hash, scope_json, scope_digest, scope_generation, view_id,
        view_revision, server_sequence, terminal_sequence, entry_count, content_bytes, digest
      FROM effect_local_server_scoped_snapshots WHERE snapshot_id = ${snapshotId}`
  })
  const findClientSnapshot = SqlSchema.findOneOption({
    Request: Schema.Struct({ spaceId: Identity.SpaceId, clientId: Identity.ClientId }),
    Result: Rows.ScopedSnapshotManifestRow,
    execute: ({ spaceId, clientId }) =>
      sql`SELECT snapshot_id, space_id, client_id, principal_digest, definition_hash,
        schema_version, schema_hash, scope_json, scope_digest, scope_generation, view_id,
        view_revision, server_sequence, terminal_sequence, entry_count, content_bytes, digest
      FROM effect_local_server_scoped_snapshots WHERE space_id = ${spaceId} AND client_id = ${clientId}`
  })
  const findSnapshotEntryPage = SqlSchema.findAll({
    Request: Schema.Struct({
      snapshotId: Identity.SnapshotId,
      afterOrdinal: Schema.Int,
      limit: Schema.Int
    }),
    Result: Rows.ServerScopedSnapshotEntryRow,
    execute: ({ snapshotId, afterOrdinal, limit }) =>
      sql`SELECT ordinal, change_json, entry_bytes, source_model, source_model_version,
        source_entity_key, source_value_json
      FROM effect_local_server_scoped_snapshot_entries
      WHERE snapshot_id = ${snapshotId} AND ordinal > ${afterOrdinal}
      ORDER BY ordinal LIMIT ${limit}`
  })

  const digest = (value: unknown) => Canonical.digest(value).pipe(Effect.provideService(Crypto.Crypto, options.crypto))

  const principalDigest = (principal: typeof Schema.Json.Type) =>
    digest({ format: 1, principal }).pipe(Effect.map((value) => Protocol.MutationDigest.make(value)))

  const scopeDigest = (scope: Protocol.ReplicationScope) =>
    Protocol.replicationScopeDigest(scope).pipe(Effect.provideService(Crypto.Crypto, options.crypto))

  const lockSpace = (spaceId: Identity.SpaceId) =>
    sql`INSERT INTO effect_local_server_space_counts (space_id, history_count, receipt_count)
      VALUES (${spaceId}, 0, 0) ON CONFLICT (space_id) DO NOTHING`.pipe(
      Effect.andThen(findSpace(spaceId)),
      Effect.mapError(StorageUnavailable.make)
    )

  const validatePreparedSpace = (
    expectedGeneration: number,
    space: typeof Rows.ReplicationSpaceRow.Type
  ) => {
    if (
      space.schema_generation !== expectedGeneration || space.active_schema_generation !== expectedGeneration ||
      space.target_schema_version !== null || space.target_schema_hash !== null || space.migration_hash !== null
    ) {
      let actual = space.schema_generation
      if (actual === expectedGeneration) actual = space.active_schema_generation
      return Effect.fail(
        new ReplicaError.SchemaGenerationConflict({
          expected: expectedGeneration,
          actual
        })
      )
    }
    if (
      space.schema_version !== options.definition.schemaIdentity.version ||
      space.schema_hash !== options.definition.schemaIdentity.hash
    ) {
      return Effect.fail(
        new ReplicaError.StaleSchema({
          expectedVersion: options.definition.schemaIdentity.version,
          expectedHash: options.definition.schemaIdentity.hash,
          actualVersion: space.schema_version,
          actualHash: space.schema_hash
        })
      )
    }
    if (space.definition_hash === options.definition.hash) return Effect.void
    return Effect.fail(
      new ReplicaError.DefinitionMismatch({
        expected: options.definition.hash,
        actual: space.definition_hash
      })
    )
  }

  const decodeAuthoritative = (
    spaceId: Identity.SpaceId,
    rows: ReadonlyArray<typeof Rows.ServerEntityRow.Type>
  ) =>
    Effect.gen(function*() {
      if (rows.length > options.maximumSnapshotEntities) {
        return yield* new ReplicaError.CapacityExceeded({
          resource: "snapshot entities",
          limit: options.maximumSnapshotEntities
        })
      }
      const entities = new Map<string, MaterializedEntity>()
      let contentBytes = 0
      for (const row of rows) {
        const model = options.definition.modelByName.get(row.model)
        if (model === undefined || row.model_version !== model.version) {
          return yield* new ReplicaError.StorageCorrupt({
            message: `Space ${spaceId} contains an invalid model row for ${row.model}`
          })
        }
        const key = yield* Codec.parse(row.entity_key).pipe(
          Effect.flatMap((parsed) => Codec.decode(Schema.Json, parsed))
        )
        const value = yield* Codec.parse(row.value_json).pipe(
          Effect.flatMap((parsed) => Codec.decode(Schema.Json, parsed))
        )
        yield* Codec.decode(model.key, key)
        yield* Codec.decode(model.schema, value)
        const keyJson = yield* Codec.stringify(key)
        const valueJson = yield* Codec.stringify(value)
        if (keyJson !== row.entity_key || valueJson !== row.value_json) {
          return yield* new ReplicaError.StorageCorrupt({
            message: `Space ${spaceId} contains a noncanonical entity row`
          })
        }
        const entity = Protocol.EntityKey.make({ model: row.model, modelVersion: row.model_version, key })
        const bytes = yield* Protocol.encodedBytesEffect({
          model: row.model,
          modelVersion: row.model_version,
          key,
          value
        })
        if (row.entity_bytes !== 0 && row.entity_bytes !== bytes) {
          return yield* new ReplicaError.StorageCorrupt({
            message: `Space ${spaceId} entity byte metadata is inconsistent`
          })
        }
        contentBytes += bytes
        if (contentBytes > options.maximumSnapshotBytes) {
          return yield* new ReplicaError.CapacityExceeded({
            resource: "snapshot bytes",
            limit: options.maximumSnapshotBytes
          })
        }
        entities.set(identityOf(row.model, keyJson), {
          identity: identityOf(row.model, keyJson),
          entityKey: keyJson,
          entity,
          value,
          valueJson,
          sourceEntity: entity,
          sourceValue: value,
          sourceEntityKey: keyJson,
          sourceValueJson: valueJson
        })
      }
      return entities
    })

  const authoritative = (spaceId: Identity.SpaceId) =>
    findEntities({ spaceId, limit: options.maximumSnapshotEntities + 1 }).pipe(
      Effect.mapError(StorageUnavailable.make),
      Effect.flatMap((rows) => decodeAuthoritative(spaceId, rows))
    )

  const projectAll = (
    source: ReadonlyMap<string, MaterializedEntity>,
    targetDefinition: Definition.Any
  ) =>
    Effect.gen(function*() {
      const all = new Map<string, MaterializedEntity>()
      for (const candidate of source.values()) {
        const projected = yield* options.projectEntity(targetDefinition, candidate.entity, candidate.value)
        if (Option.isNone(projected)) continue
        const entityKey = yield* Codec.stringify(projected.value.entity.key)
        const identity = identityOf(projected.value.entity.model, entityKey)
        if (all.has(identity)) {
          return yield* new ReplicaError.SchemaKeyCollision({
            model: projected.value.entity.model,
            key: entityKey
          })
        }
        const materialized: MaterializedEntity = {
          identity,
          entityKey,
          entity: projected.value.entity,
          value: projected.value.value,
          valueJson: yield* Codec.stringify(projected.value.value),
          sourceEntity: candidate.entity,
          sourceValue: candidate.value,
          sourceEntityKey: candidate.entityKey,
          sourceValueJson: candidate.valueJson
        }
        all.set(identity, materialized)
      }
      return all
    })

  const projectVisible = (
    request: Protocol.PullRequest | Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type,
    source: ReadonlyMap<string, MaterializedEntity>,
    targetDefinition: Definition.Any
  ) =>
    Effect.gen(function*() {
      const all = yield* projectAll(source, targetDefinition)
      const selected = new Set(request.scope.models)
      const target = new Map<string, MaterializedEntity>()
      for (const materialized of all.values()) {
        if (!selected.has(materialized.entity.model)) continue
        if (
          yield* options.authorization.entity(
            request,
            principal,
            materialized.sourceEntity,
            materialized.sourceValue
          )
        ) target.set(materialized.identity, materialized)
      }
      return { all, target }
    })

  const manifestFromRow = (row: typeof Rows.ScopedSnapshotManifestRow.Type) =>
    Protocol.SnapshotManifest.make({
      spaceId: row.space_id,
      clientId: row.client_id,
      definitionHash: row.definition_hash,
      schema: Identity.SchemaIdentity.make({ version: row.schema_version, hash: row.schema_hash }),
      scopeDigest: row.scope_digest,
      scopeGeneration: row.scope_generation,
      cursor: Protocol.ReplicationCursor.make({ viewId: row.view_id, revision: row.view_revision }),
      snapshotId: row.snapshot_id,
      sequence: row.server_sequence,
      terminalSequenceThrough: row.terminal_sequence,
      entityCount: row.entry_count,
      contentBytes: row.content_bytes,
      digest: row.digest
    })

  const decodeSnapshotEntries = (rows: ReadonlyArray<typeof Rows.ServerScopedSnapshotEntryRow.Type>) =>
    Effect.forEach(rows, (row) =>
      Codec.parse(row.change_json).pipe(
        Effect.flatMap((value) => Codec.decode(Protocol.SnapshotEntry, value)),
        Effect.flatMap((entry) =>
          Effect.gen(function*() {
            if (
              entry.ordinal !== row.ordinal || row.entry_bytes !== entry.entryBytes ||
              entry.entryBytes !== Protocol.encodedBytes(entry.change)
            ) {
              return yield* Effect.fail(
                new ReplicaError.StorageCorrupt({ message: `Scoped snapshot entry ${row.ordinal} is corrupt` })
              )
            }
            const model = options.definition.modelByName.get(row.source_model)
            if (model === undefined || model.version !== row.source_model_version) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Scoped snapshot entry ${row.ordinal} has invalid source metadata`
              })
            }
            const sourceKey = yield* Codec.parse(row.source_entity_key).pipe(
              Effect.flatMap((value) => Codec.decode(model.key, value))
            )
            const sourceValue = yield* Codec.parse(row.source_value_json).pipe(
              Effect.flatMap((value) => Codec.decode(model.schema, value))
            )
            const sourceEntityKey = yield* Codec.stringify(sourceKey)
            const sourceValueJson = yield* Codec.stringify(sourceValue)
            if (sourceEntityKey !== row.source_entity_key || sourceValueJson !== row.source_value_json) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Scoped snapshot entry ${row.ordinal} has noncanonical source metadata`
              })
            }
            return {
              entry,
              sourceModel: row.source_model,
              sourceIdentity: identityOf(row.source_model, sourceEntityKey),
              sourceEntityKey,
              sourceValueJson
            } satisfies StoredSnapshotEntry
          })
        )
      ))

  const deleteSnapshot = (spaceId: Identity.SpaceId, clientId: Identity.ClientId) =>
    Effect.gen(function*() {
      yield* sql`DELETE FROM effect_local_server_scoped_snapshot_entries WHERE snapshot_id IN (
        SELECT snapshot_id FROM effect_local_server_scoped_snapshots
        WHERE space_id = ${spaceId} AND client_id = ${clientId}
      )`
      yield* sql`DELETE FROM effect_local_server_scoped_snapshots
        WHERE space_id = ${spaceId} AND client_id = ${clientId}`
    })

  const replaceViewEntities = (
    request: Protocol.PullRequest | Protocol.BootstrapRequest,
    principal: Protocol.MutationDigest,
    cursor: Protocol.ReplicationCursor,
    entities: ReadonlyArray<MaterializedEntity>
  ) =>
    Effect.gen(function*() {
      yield* sql`DELETE FROM effect_local_server_replication_view_entities
        WHERE space_id = ${request.spaceId} AND client_id = ${request.clientId}`
      const rows = entities.map((entity) => ({
        space_id: request.spaceId,
        client_id: request.clientId,
        principal_digest: principal,
        view_id: cursor.viewId,
        model: entity.entity.model,
        model_version: entity.entity.modelVersion,
        entity_key: entity.entityKey,
        disposition: "Upsert",
        value_json: entity.valueJson
      }))
      for (let offset = 0; offset < rows.length; offset += 100) {
        yield* sql`INSERT INTO effect_local_server_replication_view_entities
          ${sql.insert(rows.slice(offset, offset + 100))}`
      }
    })

  const createSnapshot = (
    request: Protocol.PullRequest | Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type,
    principalHash: Protocol.MutationDigest
  ) =>
    Effect.gen(function*() {
      const targetDefinition = yield* options.resolveDefinition(request.schema)
      const normalized = yield* Protocol.validateReplicationScope(targetDefinition, request.scope)
      const normalizedDigest = yield* scopeDigest(normalized)
      const space = yield* findSpace(request.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
      const previous = yield* findView({ spaceId: request.spaceId, clientId: request.clientId }).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      if (Option.isSome(previous) && request.scopeGeneration < previous.value.scope_generation) {
        return yield* new ReplicaError.StaleReplicationScope({
          expected: previous.value.scope_generation,
          actual: request.scopeGeneration
        })
      }
      const source = yield* authoritative(request.spaceId)
      const projected = yield* projectVisible({ ...request, scope: normalized }, principal, source, targetDefinition)
      const visibleEntities = Array.from(projected.target.values())
      const orderedEntities = visibleEntities.toSorted((left, right) => {
        if (left.identity < right.identity) return -1
        if (left.identity > right.identity) return 1
        return 0
      })
      const orderedChanges = orderedEntities
        .map((entity) => Protocol.Upsert.make({ entity: entity.entity, value: entity.value }))
      const viewId = yield* Identity.makeReplicationViewId.pipe(
        Effect.provideService(Crypto.Crypto, options.crypto),
        Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause }))
      )
      const snapshotId = yield* Identity.makeSnapshotId.pipe(
        Effect.provideService(Crypto.Crypto, options.crypto),
        Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause }))
      )
      const cursor = Protocol.ReplicationCursor.make({
        viewId,
        revision: Identity.ReplicationViewRevision.make(0)
      })
      const entries: Array<Protocol.SnapshotEntry> = []
      let bytes = 0
      let rolling = Protocol.initialSnapshotDigest
      for (let ordinal = 0; ordinal < orderedChanges.length; ordinal++) {
        const entry = Protocol.SnapshotEntry.make({
          ordinal,
          change: orderedChanges[ordinal],
          entryBytes: yield* Protocol.encodedBytesEffect(orderedChanges[ordinal])
        })
        bytes += entry.entryBytes
        if (bytes > options.maximumSnapshotBytes) {
          return yield* new ReplicaError.CapacityExceeded({
            resource: "scoped snapshot bytes",
            limit: options.maximumSnapshotBytes
          })
        }
        rolling = yield* Protocol.snapshotEntryDigest(rolling, entry).pipe(
          Effect.provideService(Crypto.Crypto, options.crypto)
        )
        entries.push(entry)
      }
      yield* deleteSnapshot(request.spaceId, request.clientId)
      yield* sql`DELETE FROM effect_local_server_replication_pages
        WHERE space_id = ${request.spaceId} AND client_id = ${request.clientId}`
      yield* sql`INSERT INTO effect_local_server_replication_views
        (space_id, client_id, principal_digest, view_id, view_revision, scope_generation,
          scope_json, scope_digest, definition_hash, schema_version, schema_hash, server_sequence)
        VALUES (${request.spaceId}, ${request.clientId}, ${principalHash}, ${viewId}, 0,
          ${request.scopeGeneration}, ${yield* Codec.stringify(normalized)}, ${normalizedDigest},
          ${targetDefinition.hash}, ${targetDefinition.schemaIdentity.version}, ${targetDefinition.schemaIdentity.hash},
          ${space.next_server_sequence - 1})
        ON CONFLICT (space_id, client_id) DO UPDATE SET principal_digest = excluded.principal_digest,
          view_id = excluded.view_id, view_revision = excluded.view_revision,
          scope_generation = excluded.scope_generation, scope_json = excluded.scope_json,
          scope_digest = excluded.scope_digest, definition_hash = excluded.definition_hash,
          schema_version = excluded.schema_version, schema_hash = excluded.schema_hash,
          server_sequence = excluded.server_sequence`
      yield* replaceViewEntities(request, principalHash, cursor, visibleEntities)
      yield* sql`INSERT INTO effect_local_server_scoped_snapshots
        (snapshot_id, space_id, client_id, principal_digest, definition_hash, schema_version,
          schema_hash, scope_json, scope_digest, scope_generation, view_id, view_revision,
          server_sequence, terminal_sequence, entry_count, content_bytes, digest)
        VALUES (${snapshotId}, ${request.spaceId}, ${request.clientId}, ${principalHash},
          ${targetDefinition.hash}, ${targetDefinition.schemaIdentity.version}, ${targetDefinition.schemaIdentity.hash},
          ${yield* Codec.stringify(normalized)}, ${normalizedDigest}, ${request.scopeGeneration},
          ${viewId}, 0, ${space.next_server_sequence - 1}, ${space.next_terminal_sequence - 1},
          ${entries.length}, ${bytes}, ${rolling})`
      const entryRows = yield* Effect.forEach(entries, (entry) =>
        Codec.stringify(entry).pipe(Effect.map((changeJson) => ({
          snapshot_id: snapshotId,
          ordinal: entry.ordinal,
          change_json: changeJson,
          entry_bytes: entry.entryBytes,
          source_model: orderedEntities[entry.ordinal].sourceEntity.model,
          source_model_version: orderedEntities[entry.ordinal].sourceEntity.modelVersion,
          source_entity_key: orderedEntities[entry.ordinal].sourceEntityKey,
          source_value_json: orderedEntities[entry.ordinal].sourceValueJson
        }))))
      for (let offset = 0; offset < entryRows.length; offset += 100) {
        yield* sql`INSERT INTO effect_local_server_scoped_snapshot_entries
          ${sql.insert(entryRows.slice(offset, offset + 100))}`
      }
      return Protocol.SnapshotManifest.make({
        spaceId: request.spaceId,
        clientId: request.clientId,
        definitionHash: targetDefinition.hash,
        schema: targetDefinition.schemaIdentity,
        scopeDigest: normalizedDigest,
        scopeGeneration: request.scopeGeneration,
        cursor,
        snapshotId,
        sequence: Identity.ServerSequence.make(space.next_server_sequence - 1),
        terminalSequenceThrough: Identity.TerminalSequence.make(space.next_terminal_sequence - 1),
        entityCount: entries.length,
        contentBytes: bytes,
        digest: rolling
      })
    })

  const bootstrapRequired = (
    request: Protocol.PullRequest | Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type,
    principalHash: Protocol.MutationDigest
  ) =>
    createSnapshot(request, principal, principalHash).pipe(
      Effect.map((manifest) =>
        Protocol.BootstrapRequired.make({
          manifest,
          serverSchema: options.definition.schemaIdentity
        })
      )
    )

  const existingBootstrapRequired = (snapshot: typeof Rows.ScopedSnapshotManifestRow.Type) =>
    Protocol.BootstrapRequired.make({
      manifest: manifestFromRow(snapshot),
      serverSchema: options.definition.schemaIdentity
    })

  const pageFromRow = (row: typeof Rows.ReplicationPageRow.Type) =>
    Codec.parse(row.changes_json).pipe(
      Effect.flatMap((value) => Codec.decode(Schema.Array(Protocol.ViewChange), value)),
      Effect.map((changes) =>
        Protocol.PullPage.make({
          scopeGeneration: row.scope_generation,
          cursor: Protocol.ReplicationCursor.make({ viewId: row.view_id, revision: row.target_revision }),
          serverSequence: row.server_sequence,
          changes,
          contentBytes: row.content_bytes,
          digest: row.digest,
          hasMore: row.has_more === 1,
          serverSchema: options.definition.schemaIdentity
        })
      )
    )

  const pageSafe = (
    request: Protocol.PullRequest,
    principal: typeof Schema.Json.Type,
    page: Protocol.PullPage,
    all: ReadonlyMap<string, MaterializedEntity>
  ) =>
    Effect.gen(function*() {
      for (const change of page.changes) {
        const key = yield* Codec.stringify(change.entity.key)
        const current = all.get(identityOf(change.entity.model, key))
        if (change._tag === "Upsert") {
          if (
            current === undefined || !request.scope.models.includes(current.entity.model) ||
            change.entity.modelVersion !== current.entity.modelVersion ||
            (yield* Codec.stringify(change.value)) !== current.valueJson ||
            !(yield* options.authorization.entity(request, principal, current.sourceEntity, current.sourceValue))
          ) return false
        }
        if (change._tag === "Delete" && current !== undefined) return false
        if (change._tag === "Retract") {
          if (current === undefined) return false
          if (
            request.scope.models.includes(current.entity.model) &&
            (yield* options.authorization.entity(request, principal, current.sourceEntity, current.sourceValue))
          ) return false
        }
      }
      return true
    })

  const applyAcknowledgedPage = (
    request: Protocol.PullRequest,
    principalHash: Protocol.MutationDigest,
    page: Protocol.PullPage,
    scopeJson: string,
    scopeHash: Protocol.MutationDigest
  ) =>
    Effect.gen(function*() {
      const upserts: Array<Record<string, unknown>> = []
      const removals: Array<{ readonly model: string; readonly key: string }> = []
      for (const change of page.changes) {
        const key = yield* Codec.stringify(change.entity.key)
        if (change._tag === "Upsert") {
          upserts.push({
            space_id: request.spaceId,
            client_id: request.clientId,
            principal_digest: principalHash,
            view_id: page.cursor.viewId,
            model: change.entity.model,
            model_version: change.entity.modelVersion,
            entity_key: key,
            disposition: change._tag,
            value_json: yield* Codec.stringify(change.value)
          })
        } else {
          removals.push({ model: change.entity.model, key })
        }
      }
      for (let offset = 0; offset < upserts.length; offset += 100) {
        yield* sql`INSERT INTO effect_local_server_replication_view_entities
          ${sql.insert(upserts.slice(offset, offset + 100))}
          ON CONFLICT (space_id, client_id, view_id, model, entity_key) DO UPDATE SET
            principal_digest = excluded.principal_digest, model_version = excluded.model_version,
            disposition = excluded.disposition, value_json = excluded.value_json`
      }
      for (let offset = 0; offset < removals.length; offset += 100) {
        const batch = removals.slice(offset, offset + 100)
        yield* sql`DELETE FROM effect_local_server_replication_view_entities
          WHERE space_id = ${request.spaceId} AND client_id = ${request.clientId}
            AND view_id = ${page.cursor.viewId}
            AND ${sql.or(batch.map((entity) => sql`(model = ${entity.model} AND entity_key = ${entity.key})`))}`
      }
      yield* sql`UPDATE effect_local_server_replication_views SET
        view_revision = ${page.cursor.revision}, scope_generation = ${page.scopeGeneration},
        scope_json = ${scopeJson}, scope_digest = ${scopeHash}, server_sequence = ${page.serverSequence}
        WHERE space_id = ${request.spaceId} AND client_id = ${request.clientId}
          AND principal_digest = ${principalHash} AND view_id = ${page.cursor.viewId}`
      yield* sql`DELETE FROM effect_local_server_replication_pages
        WHERE space_id = ${request.spaceId} AND client_id = ${request.clientId}`
    })

  const diff = (
    request: Protocol.PullRequest,
    acknowledged: ReadonlyArray<typeof Rows.ReplicationViewEntityRow.Type>,
    all: ReadonlyMap<string, MaterializedEntity>,
    target: ReadonlyMap<string, MaterializedEntity>
  ) =>
    Effect.gen(function*() {
      const changes = new Map<string, Protocol.ViewChange>()
      const prior = new Map(acknowledged.map((row) => [identityOf(row.model, row.entity_key), row]))
      for (const entity of target.values()) {
        const row = prior.get(entity.identity)
        if (row?.disposition !== "Upsert" || row.value_json !== entity.valueJson) {
          changes.set(entity.identity, Protocol.Upsert.make({ entity: entity.entity, value: entity.value }))
        }
      }
      for (const row of acknowledged) {
        const identity = identityOf(row.model, row.entity_key)
        if (target.has(identity)) continue
        const entity = Protocol.EntityKey.make({
          model: row.model,
          modelVersion: row.model_version,
          key: yield* Codec.parse(row.entity_key)
        })
        let disposition: Protocol.ViewChange = Protocol.Delete.make({ entity })
        if (all.has(identity)) disposition = Protocol.Retract.make({ entity })
        if (row.disposition !== disposition._tag) changes.set(identity, disposition)
      }
      return [...changes.entries()].toSorted(([left], [right]) => {
        if (left < right) return -1
        if (left > right) return 1
        return 0
      })
        .map(([, change]) => change)
    })

  const persistNextPage = (
    request: Protocol.PullRequest,
    principalHash: Protocol.MutationDigest,
    view: typeof Rows.ReplicationViewRow.Type,
    changes: ReadonlyArray<Protocol.ViewChange>,
    serverSequence: Identity.ServerSequence,
    normalized: Protocol.ReplicationScope,
    normalizedDigest: Protocol.MutationDigest
  ) =>
    Effect.gen(function*() {
      const cursor = Protocol.ReplicationCursor.make({
        viewId: view.view_id,
        revision: Identity.ReplicationViewRevision.make(view.view_revision + 1)
      })
      let lower = 0
      let upper = Math.min(request.limit, changes.length)
      while (lower < upper) {
        const length = Math.ceil((lower + upper) / 2)
        const candidate = changes.slice(0, length)
        const candidatePage = Protocol.PullPage.make({
          scopeGeneration: request.scopeGeneration,
          cursor,
          serverSequence,
          changes: candidate,
          contentBytes: Protocol.encodedBytes(candidate),
          digest: Protocol.MutationDigest.make("0".repeat(64)),
          hasMore: candidate.length < changes.length,
          serverSchema: options.definition.schemaIdentity
        })
        if (Protocol.encodedBytes(candidatePage) <= Protocol.maximumBatchBytes) lower = length
        else upper = length - 1
      }
      const selected = changes.slice(0, lower)
      if (changes.length > 0 && selected.length === 0) {
        return yield* new ReplicaError.CapacityExceeded({
          resource: "replication page bytes",
          limit: Protocol.maximumBatchBytes
        })
      }
      const contentBytes = yield* Protocol.encodedBytesEffect(selected)
      const page = Protocol.PullPage.make({
        scopeGeneration: request.scopeGeneration,
        cursor,
        serverSequence,
        changes: selected,
        contentBytes,
        digest: yield* Protocol.viewChangesDigest(selected).pipe(
          Effect.provideService(Crypto.Crypto, options.crypto)
        ),
        hasMore: selected.length < changes.length,
        serverSchema: options.definition.schemaIdentity
      })
      const scopeJson = yield* Codec.stringify(normalized)
      let hasMore = 0
      if (page.hasMore) hasMore = 1
      yield* sql`INSERT INTO effect_local_server_replication_pages
        (space_id, client_id, principal_digest, view_id, base_revision, target_revision,
          scope_generation, scope_json, scope_digest, server_sequence, changes_json,
          content_bytes, digest, has_more)
        VALUES (${request.spaceId}, ${request.clientId}, ${principalHash}, ${view.view_id},
          ${view.view_revision}, ${page.cursor.revision}, ${request.scopeGeneration}, ${scopeJson},
          ${normalizedDigest}, ${serverSequence}, ${yield* Codec.stringify(selected)}, ${contentBytes},
          ${page.digest}, ${hasMore})`
      yield* sql`UPDATE effect_local_server_replication_views SET
        scope_generation = ${request.scopeGeneration}, scope_json = ${scopeJson},
        scope_digest = ${normalizedDigest}
        WHERE space_id = ${request.spaceId} AND client_id = ${request.clientId}`
      return page
    })

  const pull = (
    request: Protocol.PullRequest,
    principal: typeof Schema.Json.Type,
    expectedGeneration: number
  ) =>
    sql.withTransaction(Effect.gen(function*() {
      yield* options.authorization.scope(request, principal)
      const space = yield* lockSpace(request.spaceId)
      yield* validatePreparedSpace(expectedGeneration, space)
      const targetDefinition = yield* options.resolveDefinition(request.schema)
      const normalized = yield* Protocol.validateReplicationScope(targetDefinition, request.scope)
      const normalizedDigest = yield* scopeDigest(normalized)
      const principalHash = yield* principalDigest(principal)
      const stored = yield* findView({ spaceId: request.spaceId, clientId: request.clientId }).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      if (Option.isSome(stored) && request.scopeGeneration < stored.value.scope_generation) {
        return yield* new ReplicaError.StaleReplicationScope({
          expected: stored.value.scope_generation,
          actual: request.scopeGeneration
        })
      }
      if (
        request.cursor === null && Option.isSome(stored) && stored.value.principal_digest === principalHash &&
        stored.value.definition_hash === targetDefinition.hash &&
        stored.value.schema_version === request.schema.version && stored.value.schema_hash === request.schema.hash &&
        stored.value.scope_generation === request.scopeGeneration && stored.value.scope_digest === normalizedDigest
      ) {
        const snapshot = yield* findClientSnapshot({
          spaceId: request.spaceId,
          clientId: request.clientId
        }).pipe(Effect.mapError(StorageUnavailable.make))
        if (
          Option.isSome(snapshot) && snapshot.value.principal_digest === principalHash &&
          snapshot.value.definition_hash === targetDefinition.hash &&
          snapshot.value.schema_version === request.schema.version &&
          snapshot.value.schema_hash === request.schema.hash &&
          snapshot.value.scope_generation === request.scopeGeneration &&
          snapshot.value.scope_digest === normalizedDigest && snapshot.value.view_id === stored.value.view_id &&
          snapshot.value.view_revision === stored.value.view_revision &&
          snapshot.value.server_sequence === space.next_server_sequence - 1 &&
          snapshot.value.terminal_sequence === space.next_terminal_sequence - 1
        ) return existingBootstrapRequired(snapshot.value)
      }
      if (
        request.cursor === null || Option.isNone(stored) || stored.value.principal_digest !== principalHash ||
        stored.value.definition_hash !== targetDefinition.hash ||
        stored.value.schema_version !== request.schema.version || stored.value.schema_hash !== request.schema.hash ||
        request.cursor.viewId !== stored.value.view_id || request.scopeGeneration < stored.value.scope_generation
      ) return yield* bootstrapRequired({ ...request, scope: normalized }, principal, principalHash)
      const view = stored.value
      if (request.scopeGeneration === view.scope_generation && view.scope_digest !== normalizedDigest) {
        return yield* new ReplicaError.ProtocolInvalid({
          message: "Replication scope changed without advancing scope generation"
        })
      }
      const pageRow = yield* findPage({ spaceId: request.spaceId, clientId: request.clientId }).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      const source = yield* authoritative(request.spaceId)
      const projected = yield* projectVisible({ ...request, scope: normalized }, principal, source, targetDefinition)
      if (Option.isSome(pageRow)) {
        const row = pageRow.value
        if (
          row.principal_digest !== principalHash || row.view_id !== view.view_id
        ) return yield* bootstrapRequired({ ...request, scope: normalized }, principal, principalHash)
        const page = yield* pageFromRow(row)
        const acknowledgesPriorGeneration = request.cursor.revision === row.target_revision &&
          request.scopeGeneration > row.scope_generation
        if (
          !acknowledgesPriorGeneration &&
          (row.scope_generation !== request.scopeGeneration || row.scope_digest !== normalizedDigest)
        ) return yield* bootstrapRequired({ ...request, scope: normalized }, principal, principalHash)
        if (request.cursor.revision === row.base_revision) {
          if (!(yield* pageSafe({ ...request, scope: normalized }, principal, page, projected.all))) {
            return yield* bootstrapRequired({ ...request, scope: normalized }, principal, principalHash)
          }
          return page
        }
        if (request.cursor.revision !== row.target_revision) {
          return yield* bootstrapRequired({ ...request, scope: normalized }, principal, principalHash)
        }
        yield* applyAcknowledgedPage(request, principalHash, page, row.scope_json, row.scope_digest)
      } else if (request.cursor.revision !== view.view_revision) {
        return yield* bootstrapRequired({ ...request, scope: normalized }, principal, principalHash)
      }
      const currentView = yield* findView({ spaceId: request.spaceId, clientId: request.clientId }).pipe(
        Effect.mapError(StorageUnavailable.make),
        Effect.flatMap(Option.match({
          onNone: () => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Replication view disappeared" })),
          onSome: Effect.succeed
        }))
      )
      const acknowledged = yield* findViewEntities({
        spaceId: request.spaceId,
        clientId: request.clientId,
        viewId: currentView.view_id
      }).pipe(Effect.mapError(StorageUnavailable.make))
      const changes = yield* diff(
        { ...request, scope: normalized },
        acknowledged,
        projected.all,
        projected.target
      )
      return yield* persistNextPage(
        request,
        principalHash,
        currentView,
        changes,
        Identity.ServerSequence.make(space.next_server_sequence - 1),
        normalized,
        normalizedDigest
      )
    })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

  const bootstrap = (
    request: Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type,
    expectedGeneration: number
  ) =>
    sql.withTransaction(Effect.gen(function*() {
      yield* options.authorization.scope(request, principal)
      const space = yield* lockSpace(request.spaceId)
      yield* validatePreparedSpace(expectedGeneration, space)
      const targetDefinition = yield* options.resolveDefinition(request.schema)
      const normalized = yield* Protocol.validateReplicationScope(targetDefinition, request.scope)
      const normalizedDigest = yield* scopeDigest(normalized)
      const principalHash = yield* principalDigest(principal)
      let stored = yield* findSnapshot(request.snapshotId).pipe(Effect.mapError(StorageUnavailable.make))
      if (
        Option.isNone(stored) || stored.value.space_id !== request.spaceId ||
        stored.value.client_id !== request.clientId || stored.value.principal_digest !== principalHash ||
        stored.value.definition_hash !== targetDefinition.hash ||
        stored.value.schema_version !== request.schema.version || stored.value.schema_hash !== request.schema.hash ||
        stored.value.scope_digest !== normalizedDigest || stored.value.scope_generation !== request.scopeGeneration ||
        stored.value.view_id !== request.cursor.viewId || stored.value.view_revision !== request.cursor.revision
      ) {
        const manifest = yield* createSnapshot({ ...request, scope: normalized }, principal, principalHash)
        stored = yield* findSnapshot(manifest.snapshotId).pipe(Effect.mapError(StorageUnavailable.make))
      }
      if (Option.isNone(stored)) {
        return yield* new ReplicaError.StorageCorrupt({ message: "Scoped snapshot disappeared" })
      }
      let row = stored.value
      let afterOrdinal = request.afterOrdinal
      if (afterOrdinal >= row.entry_count) {
        return yield* new ReplicaError.CursorGap({
          expected: Math.max(-1, row.entry_count - 1),
          actual: afterOrdinal
        })
      }
      const loadEntries = (snapshot: typeof Rows.ScopedSnapshotManifestRow.Type, after: number) =>
        findSnapshotEntryPage({
          snapshotId: snapshot.snapshot_id,
          afterOrdinal: after,
          limit: request.limit
        }).pipe(
          Effect.mapError(StorageUnavailable.make),
          Effect.flatMap(decodeSnapshotEntries),
          Effect.flatMap((entries) => {
            for (let index = 0; index < entries.length; index++) {
              if (entries[index].entry.ordinal !== after + index + 1) {
                return Effect.fail(
                  new ReplicaError.StorageCorrupt({ message: "Scoped snapshot page contains an ordinal gap" })
                )
              }
            }
            if (entries.length === 0 && after + 1 < snapshot.entry_count) {
              return Effect.fail(
                new ReplicaError.StorageCorrupt({ message: "Scoped snapshot page is missing durable entries" })
              )
            }
            return Effect.succeed(entries)
          })
        )
      const selectEntries = (
        snapshot: typeof Rows.ScopedSnapshotManifestRow.Type,
        after: number,
        entries: ReadonlyArray<StoredSnapshotEntry>
      ) => {
        let lower = 0
        let upper = Math.min(request.limit, entries.length)
        while (lower < upper) {
          const length = Math.ceil((lower + upper) / 2)
          const candidate = Protocol.BootstrapPage.make({
            manifest: manifestFromRow(snapshot),
            entries: entries.slice(0, length).map((snapshotEntry) => snapshotEntry.entry),
            hasMore: after + length + 1 < snapshot.entry_count,
            serverSchema: options.definition.schemaIdentity
          })
          if (Protocol.encodedBytes(candidate) <= options.maximumBootstrapPageBytes) lower = length
          else upper = length - 1
        }
        return entries.slice(0, lower)
      }
      let remaining = yield* loadEntries(row, afterOrdinal)
      let selected = selectEntries(row, afterOrdinal, remaining)
      const sourceRows = yield* findEntitiesByIdentity({
        spaceId: request.spaceId,
        entitiesJson: yield* Codec.stringify(selected.map((snapshotEntry) => ({
          model: snapshotEntry.sourceModel,
          key: snapshotEntry.sourceEntityKey
        })))
      }).pipe(Effect.mapError(StorageUnavailable.make))
      const source = yield* decodeAuthoritative(request.spaceId, sourceRows)
      let valid = true
      for (const storedEntry of selected) {
        const { entry } = storedEntry
        const current = source.get(storedEntry.sourceIdentity)
        let projected = Option.none<{ readonly entity: Protocol.EntityKey; readonly value: typeof Schema.Json.Type }>()
        if (current !== undefined) {
          projected = yield* options.projectEntity(targetDefinition, current.entity, current.value)
        }
        let projectedKey: string | undefined
        if (Option.isSome(projected)) {
          projectedKey = yield* Codec.stringify(projected.value.entity.key)
        }
        if (
          entry.change._tag !== "Upsert" || current === undefined ||
          current.valueJson !== storedEntry.sourceValueJson || Option.isNone(projected) ||
          !normalized.models.includes(projected.value.entity.model) ||
          entry.change.entity.model !== projected.value.entity.model ||
          entry.change.entity.modelVersion !== projected.value.entity.modelVersion ||
          (yield* Codec.stringify(entry.change.entity.key)) !== projectedKey ||
          (yield* Codec.stringify(entry.change.value)) !== (yield* Codec.stringify(projected.value.value)) ||
          !(yield* options.authorization.entity(request, principal, current.sourceEntity, current.sourceValue))
        ) {
          valid = false
          break
        }
      }
      if (!valid) {
        const manifest = yield* createSnapshot({ ...request, scope: normalized }, principal, principalHash)
        const replacement = yield* findSnapshot(manifest.snapshotId).pipe(Effect.mapError(StorageUnavailable.make))
        if (Option.isNone(replacement)) {
          return yield* new ReplicaError.StorageCorrupt({ message: "Replacement scoped snapshot disappeared" })
        }
        row = replacement.value
        afterOrdinal = -1
        remaining = yield* loadEntries(row, afterOrdinal)
        selected = selectEntries(row, afterOrdinal, remaining)
      }
      if (remaining.length > 0 && selected.length === 0) {
        return yield* new ReplicaError.CapacityExceeded({
          resource: "bootstrap page bytes",
          limit: options.maximumBootstrapPageBytes
        })
      }
      return Protocol.BootstrapPage.make({
        manifest: manifestFromRow(row),
        entries: selected.map((snapshotEntry) => snapshotEntry.entry),
        hasMore: afterOrdinal + selected.length + 1 < row.entry_count,
        serverSchema: options.definition.schemaIdentity
      })
    })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

  return { pull, bootstrap } as const
}
