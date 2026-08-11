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
}

interface MaterializedEntity {
  readonly identity: string
  readonly entityKey: string
  readonly entity: Protocol.EntityKey
  readonly value: typeof Schema.Json.Type
  readonly valueJson: string
}

const identityOf = (model: string, entityKey: string) => `${model}\u0000${entityKey}`

export const make = (options: Options) => {
  const { sql } = options
  const findSpace = SqlSchema.findOne({
    Request: Identity.SpaceId,
    Result: Rows.ReplicationSpaceRow,
    execute: (spaceId) =>
      sql`SELECT definition_hash, schema_version, schema_hash, active_schema_generation,
        next_server_sequence, next_terminal_sequence
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
  const findSnapshotEntries = SqlSchema.findAll({
    Request: Identity.SnapshotId,
    Result: Rows.ScopedSnapshotEntryRow,
    execute: (snapshotId) =>
      sql`SELECT ordinal, change_json, entry_bytes
      FROM effect_local_server_scoped_snapshot_entries
      WHERE snapshot_id = ${snapshotId} ORDER BY ordinal`
  })

  const digest = (value: unknown) => Canonical.digest(value).pipe(Effect.provideService(Crypto.Crypto, options.crypto))

  const principalDigest = (principal: typeof Schema.Json.Type) =>
    digest({ format: 1, principal }).pipe(Effect.map((value) => Protocol.MutationDigest.make(value)))

  const scopeDigest = (scope: Protocol.ReplicationScope) =>
    digest({ format: 1, scope }).pipe(Effect.map((value) => Protocol.MutationDigest.make(value)))

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
        const key = yield* Codec.parse(row.entity_key).pipe(Effect.flatMap((value) => Codec.decode(model.key, value)))
        const value = yield* Codec.parse(row.value_json).pipe(
          Effect.flatMap((parsed) => Codec.decode(model.schema, parsed))
        )
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
          valueJson
        })
      }
      return entities
    })

  const authoritative = (spaceId: Identity.SpaceId) =>
    findEntities({ spaceId, limit: options.maximumSnapshotEntities + 1 }).pipe(
      Effect.mapError(StorageUnavailable.make),
      Effect.flatMap((rows) => decodeAuthoritative(spaceId, rows))
    )

  const visible = (
    request: Protocol.PullRequest | Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type,
    all: ReadonlyMap<string, MaterializedEntity>
  ) =>
    Effect.gen(function*() {
      const selected = new Set(request.scope.models)
      const target = new Map<string, MaterializedEntity>()
      for (const candidate of all.values()) {
        if (!selected.has(candidate.entity.model)) continue
        if (yield* options.authorization.entity(request, principal, candidate.entity, candidate.value)) {
          target.set(candidate.identity, candidate)
        }
      }
      return target
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

  const decodeSnapshotEntries = (rows: ReadonlyArray<typeof Rows.ScopedSnapshotEntryRow.Type>) =>
    Effect.forEach(rows, (row) =>
      Codec.parse(row.change_json).pipe(
        Effect.flatMap((value) => Codec.decode(Protocol.SnapshotEntry, value)),
        Effect.flatMap((entry) => {
          if (
            entry.ordinal !== row.ordinal || row.entry_bytes !== entry.entryBytes ||
            entry.entryBytes !== Protocol.encodedBytes(entry.change)
          ) {
            return Effect.fail(
              new ReplicaError.StorageCorrupt({ message: `Scoped snapshot entry ${row.ordinal} is corrupt` })
            )
          }
          return Effect.succeed(entry)
        })
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
    changes: ReadonlyArray<Protocol.ViewChange>
  ) =>
    Effect.gen(function*() {
      yield* sql`DELETE FROM effect_local_server_replication_view_entities
        WHERE space_id = ${request.spaceId} AND client_id = ${request.clientId}`
      for (const change of changes) {
        const key = yield* Codec.stringify(change.entity.key)
        let valueJson: string | null = null
        if (change._tag === "Upsert") valueJson = yield* Codec.stringify(change.value)
        yield* sql`INSERT INTO effect_local_server_replication_view_entities
          (space_id, client_id, principal_digest, view_id, model, model_version, entity_key,
            disposition, value_json)
          VALUES (${request.spaceId}, ${request.clientId}, ${principal}, ${cursor.viewId},
            ${change.entity.model}, ${change.entity.modelVersion}, ${key}, ${change._tag}, ${valueJson})`
      }
    })

  const createSnapshot = (
    request: Protocol.PullRequest | Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type,
    principalHash: Protocol.MutationDigest
  ) =>
    Effect.gen(function*() {
      const normalized = yield* Protocol.validateReplicationScope(options.definition, request.scope)
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
      const all = yield* authoritative(request.spaceId)
      const target = yield* visible({ ...request, scope: normalized }, principal, all)
      let priorRows: ReadonlyArray<typeof Rows.ReplicationViewEntityRow.Type> = []
      if (Option.isSome(previous)) {
        priorRows = yield* findViewEntities({
          spaceId: request.spaceId,
          clientId: request.clientId,
          viewId: previous.value.view_id
        }).pipe(Effect.mapError(StorageUnavailable.make))
      }
      const changes: Array<Protocol.ViewChange> = []
      for (const entity of target.values()) {
        changes.push(Protocol.Upsert.make({ entity: entity.entity, value: entity.value }))
      }
      for (const prior of priorRows) {
        const identity = identityOf(prior.model, prior.entity_key)
        if (target.has(identity)) continue
        const current = all.get(identity)
        const entity = Protocol.EntityKey.make({
          model: prior.model,
          modelVersion: prior.model_version,
          key: yield* Codec.parse(prior.entity_key)
        })
        if (current === undefined) changes.push(Protocol.Delete.make({ entity }))
        else changes.push(Protocol.Retract.make({ entity }))
      }
      const orderedChanges = changes.toSorted((left, right) => {
        const leftKey = identityOf(left.entity.model, Canonical.stringify(left.entity.key))
        const rightKey = identityOf(right.entity.model, Canonical.stringify(right.entity.key))
        if (leftKey < rightKey) return -1
        if (leftKey > rightKey) return 1
        return 0
      })
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
        rolling = Protocol.SnapshotDigest.make(yield* digest({ previous: rolling, entry }))
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
          ${space.definition_hash}, ${space.schema_version}, ${space.schema_hash},
          ${space.next_server_sequence - 1})
        ON CONFLICT (space_id, client_id) DO UPDATE SET principal_digest = excluded.principal_digest,
          view_id = excluded.view_id, view_revision = excluded.view_revision,
          scope_generation = excluded.scope_generation, scope_json = excluded.scope_json,
          scope_digest = excluded.scope_digest, definition_hash = excluded.definition_hash,
          schema_version = excluded.schema_version, schema_hash = excluded.schema_hash,
          server_sequence = excluded.server_sequence`
      yield* replaceViewEntities(request, principalHash, cursor, changes)
      yield* sql`INSERT INTO effect_local_server_scoped_snapshots
        (snapshot_id, space_id, client_id, principal_digest, definition_hash, schema_version,
          schema_hash, scope_json, scope_digest, scope_generation, view_id, view_revision,
          server_sequence, terminal_sequence, entry_count, content_bytes, digest)
        VALUES (${snapshotId}, ${request.spaceId}, ${request.clientId}, ${principalHash},
          ${space.definition_hash}, ${space.schema_version}, ${space.schema_hash},
          ${yield* Codec.stringify(normalized)}, ${normalizedDigest}, ${request.scopeGeneration},
          ${viewId}, 0, ${space.next_server_sequence - 1}, ${space.next_terminal_sequence - 1},
          ${entries.length}, ${bytes}, ${rolling})`
      for (const entry of entries) {
        yield* sql`INSERT INTO effect_local_server_scoped_snapshot_entries
          (snapshot_id, ordinal, change_json, entry_bytes)
          VALUES (${snapshotId}, ${entry.ordinal}, ${yield* Codec.stringify(entry)}, ${entry.entryBytes})`
      }
      return Protocol.SnapshotManifest.make({
        spaceId: request.spaceId,
        clientId: request.clientId,
        definitionHash: space.definition_hash,
        schema: Identity.SchemaIdentity.make({ version: space.schema_version, hash: space.schema_hash }),
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
      Effect.map((manifest) => Protocol.BootstrapRequired.make({ manifest }))
    )

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
          hasMore: row.has_more === 1
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
            !(yield* options.authorization.entity(request, principal, current.entity, current.value))
          ) return false
        }
        if (
          change._tag === "Delete" && current !== undefined &&
          (!request.scope.models.includes(current.entity.model) ||
            !(yield* options.authorization.entity(request, principal, current.entity, current.value)))
        ) return false
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
      for (const change of page.changes) {
        const key = yield* Codec.stringify(change.entity.key)
        let valueJson: string | null = null
        if (change._tag === "Upsert") valueJson = yield* Codec.stringify(change.value)
        yield* sql`INSERT INTO effect_local_server_replication_view_entities
          (space_id, client_id, principal_digest, view_id, model, model_version, entity_key,
            disposition, value_json)
          VALUES (${request.spaceId}, ${request.clientId}, ${principalHash}, ${page.cursor.viewId},
            ${change.entity.model}, ${change.entity.modelVersion}, ${key}, ${change._tag}, ${valueJson})
          ON CONFLICT (space_id, client_id, view_id, model, entity_key) DO UPDATE SET
            principal_digest = excluded.principal_digest, model_version = excluded.model_version,
            disposition = excluded.disposition, value_json = excluded.value_json`
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
      const selected: Array<Protocol.ViewChange> = []
      for (const change of changes) {
        if (selected.length >= request.limit) break
        const candidate = [...selected, change]
        const candidatePage = Protocol.PullPage.make({
          scopeGeneration: request.scopeGeneration,
          cursor,
          serverSequence,
          changes: candidate,
          contentBytes: Protocol.encodedBytes(candidate),
          digest: Protocol.MutationDigest.make("0".repeat(64)),
          hasMore: candidate.length < changes.length
        })
        if (Protocol.encodedBytes(candidatePage) > Protocol.maximumBatchBytes) break
        selected.push(change)
      }
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
        digest: Protocol.MutationDigest.make(yield* digest({ format: 1, changes: selected })),
        hasMore: selected.length < changes.length
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

  const pull = (request: Protocol.PullRequest, principal: typeof Schema.Json.Type) =>
    sql.withTransaction(Effect.gen(function*() {
      yield* options.authorization.scope(request, principal)
      const normalized = yield* Protocol.validateReplicationScope(options.definition, request.scope)
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
        request.cursor === null || Option.isNone(stored) || stored.value.principal_digest !== principalHash ||
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
      const all = yield* authoritative(request.spaceId)
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
          if (!(yield* pageSafe({ ...request, scope: normalized }, principal, page, all))) {
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
      const target = yield* visible({ ...request, scope: normalized }, principal, all)
      const changes = yield* diff({ ...request, scope: normalized }, acknowledged, all, target)
      const space = yield* findSpace(request.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
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

  const bootstrap = (request: Protocol.BootstrapRequest, principal: typeof Schema.Json.Type) =>
    sql.withTransaction(Effect.gen(function*() {
      yield* options.authorization.scope(request, principal)
      const normalized = yield* Protocol.validateReplicationScope(options.definition, request.scope)
      const normalizedDigest = yield* scopeDigest(normalized)
      const principalHash = yield* principalDigest(principal)
      let stored = yield* findSnapshot(request.snapshotId).pipe(Effect.mapError(StorageUnavailable.make))
      if (
        Option.isNone(stored) || stored.value.space_id !== request.spaceId ||
        stored.value.client_id !== request.clientId || stored.value.principal_digest !== principalHash ||
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
      let rows = yield* findSnapshotEntries(row.snapshot_id).pipe(Effect.mapError(StorageUnavailable.make))
      let entries = yield* decodeSnapshotEntries(rows)
      const all = yield* authoritative(request.spaceId)
      let valid = true
      for (const entry of entries) {
        const key = yield* Codec.stringify(entry.change.entity.key)
        const current = all.get(identityOf(entry.change.entity.model, key))
        const authorized = current !== undefined && normalized.models.includes(current.entity.model) &&
          (yield* options.authorization.entity(request, principal, current.entity, current.value))
        if (
          (entry.change._tag === "Upsert" && !authorized) ||
          (entry.change._tag === "Delete" && current !== undefined) ||
          (entry.change._tag === "Retract" && (current === undefined || authorized))
        ) {
          valid = false
          break
        }
      }
      let afterOrdinal = request.afterOrdinal
      if (!valid) {
        const manifest = yield* createSnapshot({ ...request, scope: normalized }, principal, principalHash)
        const replacement = yield* findSnapshot(manifest.snapshotId).pipe(Effect.mapError(StorageUnavailable.make))
        if (Option.isNone(replacement)) {
          return yield* new ReplicaError.StorageCorrupt({ message: "Replacement scoped snapshot disappeared" })
        }
        row = replacement.value
        rows = yield* findSnapshotEntries(row.snapshot_id).pipe(Effect.mapError(StorageUnavailable.make))
        entries = yield* decodeSnapshotEntries(rows)
        afterOrdinal = -1
      }
      if (afterOrdinal >= entries.length) {
        return yield* new ReplicaError.CursorGap({
          expected: Math.max(-1, entries.length - 1),
          actual: afterOrdinal
        })
      }
      const remaining = entries.slice(afterOrdinal + 1)
      const selected: Array<Protocol.SnapshotEntry> = []
      for (const entry of remaining) {
        if (selected.length >= request.limit) break
        const candidate = Protocol.BootstrapPage.make({
          manifest: manifestFromRow(row),
          entries: [...selected, entry],
          hasMore: selected.length + 1 < remaining.length
        })
        if (Protocol.encodedBytes(candidate) > options.maximumBootstrapPageBytes) break
        selected.push(entry)
      }
      if (remaining.length > 0 && selected.length === 0) {
        return yield* new ReplicaError.CapacityExceeded({
          resource: "bootstrap page bytes",
          limit: options.maximumBootstrapPageBytes
        })
      }
      return Protocol.BootstrapPage.make({
        manifest: manifestFromRow(row),
        entries: selected,
        hasMore: selected.length < remaining.length
      })
    })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

  return { pull, bootstrap } as const
}
