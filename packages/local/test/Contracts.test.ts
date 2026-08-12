import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Canonical from "../src/Canonical.js"
import * as Definition from "../src/Definition.js"
import * as Field from "../src/Field.js"
import * as Model from "../src/Model.js"
import * as Mutation from "../src/Mutation.js"
import * as Protocol from "../src/Protocol.js"
import * as Query from "../src/Query.js"
import * as ReplicaError from "../src/ReplicaError.js"

const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const ListTodos = Query.make("ListTodos", { success: Schema.Array(Todo.schema), dependsOn: [Todo] })

describe("domain contracts", () => {
  it("uses JSON null as the wire representation for void payloads and results", () => {
    const mutation = Mutation.make("Touch", { version: 1 })
    const query = Query.make("Count", { dependsOn: [] })
    assert.strictEqual(Schema.encodeSync(mutation.payloadSchema)(undefined), null)
    assert.strictEqual(Schema.decodeUnknownSync(mutation.successSchema)(null), undefined)
    assert.strictEqual(Schema.encodeSync(query.payloadSchema)(undefined), null)
  })

  it("builds a stable definition hash from Schema contracts", () => {
    const first = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo], queries: [ListTodos] })
    const second = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo], queries: [ListTodos] })
    assert.strictEqual(first.hash, second.hash)
    assert.strictEqual(first.modelByName.get("Todo"), Todo)
  })

  it("rejects duplicate names and unregistered query dependencies", () => {
    assert.throws(
      () => Definition.make({ version: 1, models: [Todo, Todo], mutations: [PutTodo] }),
      /Duplicate model name/
    )
    const Other = Model.make("Other", { version: 1, key: Schema.String, schema: Schema.Struct({ id: Schema.String }) })
    const InvalidQuery = Query.make("Invalid", { dependsOn: [Other] })
    assert.throws(
      () => Definition.make({ version: 1, models: [Todo], mutations: [PutTodo], queries: [InvalidQuery] }),
      /unregistered model/
    )
  })

  it("reserves protocol names", () => {
    assert.throws(
      () => Model.make("$Model", { version: 1, key: Schema.String, schema: Schema.String }),
      /must not start/
    )
    assert.throws(() => Mutation.make("$Mutation", { version: 1 }), /must not start/)
    assert.throws(() => Query.make("$Query", { dependsOn: [] }), /must not start/)
  })

  it.effect("applies opt in field semantics without replication metadata", () =>
    Effect.gen(function*() {
      assert.strictEqual(yield* Field.counter.apply(10, { _tag: "Increment", delta: 3 }), 13)
    }))

  it.effect("deduplicates grow only set values by canonical identity", () =>
    Effect.gen(function*() {
      const semantics = Field.growOnlySet(Schema.Struct({ id: Schema.String, value: Schema.Number }))
      assert.deepStrictEqual(
        yield* semantics.apply([{ id: "a", value: 1 }], { _tag: "Add", value: { value: 1, id: "a" } }),
        [{ id: "a", value: 1 }]
      )
    }))

  it("canonicalizes object order and enforces protocol page limits", () => {
    assert.strictEqual(Canonical.stringify({ b: 2, a: 1 }), Canonical.stringify({ a: 1, b: 2 }))
    assert.throws(
      () =>
        Schema.decodeUnknownSync(Protocol.PullRequest)({
          spaceId: "spc_00000000-0000-4000-8000-000000000001",
          clientId: "cli_00000000-0000-4000-8000-000000000001",
          schema: { version: 1, hash: "0123456789abcdef" },
          scope: { models: ["Todo"] },
          scopeGeneration: 1,
          cursor: null,
          limit: Protocol.maximumBatchEntries + 1
        }),
      /less than or equal to 1000/
    )
  })

  it("decodes and normalizes a model replication scope", () => {
    const decoded = Schema.decodeUnknownSync(Protocol.ReplicationScope)({ models: ["Todo", "Other"] })
    assert.deepStrictEqual(decoded, { models: ["Todo", "Other"] })
    assert.deepStrictEqual(Protocol.normalizeReplicationScope({ models: ["Todo", "Other"] }), {
      models: ["Other", "Todo"]
    })
    assert.throws(
      () => Schema.decodeUnknownSync(Protocol.ReplicationScope)({ models: ["Todo", "Todo"] }),
      /unique/i
    )
  })

  it.effect("validates replication scope model names against the definition", () =>
    Effect.gen(function*() {
      const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo] })
      assert.deepStrictEqual(
        yield* Protocol.validateReplicationScope(definition, { models: ["Todo"] }),
        { models: ["Todo"] }
      )
      const failure = yield* Effect.flip(
        Protocol.validateReplicationScope(definition, { models: ["Missing"] })
      )
      assert.instanceOf(failure, ReplicaError.ProtocolInvalid)
      assert.match(failure.message, /Unknown replication model: Missing/)
    }))

  it("round trips scoped replication protocol values", () => {
    const spaceId = "spc_00000000-0000-4000-8000-000000000001"
    const clientId = "cli_00000000-0000-4000-8000-000000000002"
    const viewId = "viw_00000000-0000-4000-8000-000000000003"
    const schema = { version: 1, hash: "0000000000000000" }
    const scope = { models: ["Todo"] }
    const cursor = { viewId, revision: 0 }

    const pull = Schema.decodeUnknownSync(Protocol.PullRequest)({
      spaceId,
      clientId,
      schema,
      scope,
      scopeGeneration: 1,
      cursor,
      limit: 10
    })
    assert.strictEqual(pull.spaceId, spaceId)
    assert.strictEqual(pull.clientId, clientId)
    assert.deepStrictEqual(pull.scope.models, ["Todo"])
    assert.strictEqual(pull.scopeGeneration, 1)
    assert.strictEqual(pull.cursor?.viewId, viewId)
    assert.deepStrictEqual(
      Schema.decodeUnknownSync(Protocol.BootstrapRequest)({
        spaceId,
        clientId,
        schema,
        scope,
        scopeGeneration: 1,
        cursor,
        snapshotId: "snp_00000000-0000-4000-8000-000000000004",
        afterOrdinal: -1,
        limit: 10
      }).cursor,
      cursor
    )
    assert.deepStrictEqual(
      Schema.decodeUnknownSync(Protocol.WatchRequest)({
        spaceId,
        clientId,
        schema,
        scope,
        scopeGeneration: 1,
        cursor
      }).cursor,
      cursor
    )
    assert.throws(() =>
      Schema.decodeUnknownSync(Protocol.PullRequest)({
        spaceId,
        clientId,
        schema,
        scope,
        scopeGeneration: -1,
        cursor: { viewId, revision: -1 },
        limit: 10
      })
    )
  })

  it("represents revocation separately from authoritative deletion", () => {
    const entity = { model: "Todo", modelVersion: 1, key: "a" }
    const change = Schema.decodeUnknownSync(Protocol.ViewChange)({ _tag: "Retract", entity })
    assert.strictEqual(change._tag, "Retract")
    assert.deepStrictEqual(change.entity.key, "a")
  })

  it("accepts only the current mutation protocol with explicit membership", () => {
    const current = {
      spaceId: "spc_00000000-0000-4000-8000-000000000001",
      clientId: "cli_00000000-0000-4000-8000-000000000001",
      membershipIncarnation: "inc_00000000-0000-4000-8000-000000000001",
      mutationId: "mut_00000000-0000-4000-8000-000000000001",
      localSequence: 1,
      basis: 0,
      name: "PutTodo",
      payload: { id: "1", title: "current" },
      digestVersion: 3,
      sourceSchema: { version: 1, hash: "1111111111111111" },
      mutationVersion: 1,
      digest: "1".repeat(64)
    }
    assert.deepStrictEqual(Schema.decodeUnknownSync(Protocol.MutationEnvelope)(current).digestVersion, 3)
    assert.throws(() => Schema.decodeUnknownSync(Protocol.MutationEnvelope)({ ...current, digestVersion: 2 }))
    assert.throws(() =>
      Schema.decodeUnknownSync(Protocol.MutationEnvelope)({ ...current, membershipIncarnation: undefined })
    )
  })

  it("requires a negotiated protocol version on every operation payload", () => {
    const spaceId = "spc_00000000-0000-4000-8000-000000000001"
    const schema = Definition.make({ version: 1, models: [], mutations: [] }).schemaIdentity
    const envelope = {
      spaceId,
      clientId: "cli_00000000-0000-4000-8000-000000000001",
      membershipIncarnation: "inc_00000000-0000-4000-8000-000000000001",
      mutationId: "mut_00000000-0000-4000-8000-000000000001",
      localSequence: 1,
      basis: 0,
      name: "Put",
      payload: null,
      digestVersion: 3,
      sourceSchema: schema,
      mutationVersion: 1,
      digest: "0".repeat(64)
    }
    const operations = [
      [Protocol.VersionedSubmitRequest, { envelope, schema }],
      [Protocol.VersionedDiscardRequest, { envelope, schema }],
      [Protocol.VersionedPullRequest, { spaceId, schema, after: 0, limit: 1 }],
      [Protocol.VersionedWatchRequest, { spaceId, schema }],
      [Protocol.VersionedBootstrapRequest, {
        spaceId,
        schema,
        snapshotId: "snp_00000000-0000-4000-8000-000000000001",
        afterOrdinal: -1,
        limit: 1
      }],
      [Protocol.VersionedPresenceUpdate, {
        spaceId,
        clientId: envelope.clientId,
        value: null,
        ttlMillis: 1
      }],
      [Protocol.VersionedWatchPresenceRequest, { spaceId }]
    ] as const

    for (const [contract, operation] of operations) {
      assert.throws(() => Schema.decodeUnknownSync(contract)(operation))
      assert.doesNotThrow(() =>
        Schema.decodeUnknownSync(contract)({
          ...operation,
          protocolVersion: Protocol.currentProtocolVersion
        })
      )
    }
  })
})
