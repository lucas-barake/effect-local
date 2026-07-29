import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as DocumentSet from "../src/DocumentSet.js"
import * as Identity from "../src/Identity.js"
import * as Mutation from "../src/Mutation.js"
import * as Projection from "../src/Projection.js"
import * as Query from "../src/Query.js"
import * as ReplicaDefinition from "../src/ReplicaDefinition.js"

describe("ReplicaDefinition guards", () => {
  const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const TaskRows = Projection.make("TaskRows", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ title: Schema.String }),
    key: (row) => row.title,
    project: (snapshot) => [{ title: snapshot.value.title }]
  })
  const ListTasks = Query.make("ListTasks", { success: Schema.Array(TaskRows.Row), dependsOn: [TaskRows] })

  it("constructs exact document invalidation keys for every semantic scope", () => {
    assert.strictEqual(ReplicaDefinition.documentTypeQueryKey("Task"), "Task")
    assert.strictEqual(
      ReplicaDefinition.documentInstanceKey("Task", documentId),
      "[\"@lucas-barake/effect-local/invalidation\",\"document-instance\",\"Task\",\"doc_00000000-0000-4000-8000-000000000001\"]"
    )
    assert.strictEqual(
      ReplicaDefinition.documentTypeRefreshKey("Task"),
      "[\"@lucas-barake/effect-local/invalidation\",\"document-type-refresh\",\"Task\"]"
    )
    assert.deepStrictEqual(ReplicaDefinition.documentCommitKeys("Task", documentId), [
      "Task",
      "[\"@lucas-barake/effect-local/invalidation\",\"document-instance\",\"Task\",\"doc_00000000-0000-4000-8000-000000000001\"]"
    ])
  })

  it("keeps structured keys collision safe for arbitrary document names", () => {
    const names = [
      " ",
      "a",
      "a:b",
      "a:b:c",
      "[\"a\",\"b\"]",
      "Task\",\"doc_00000000-0000-4000-8000-000000000001",
      "Task\nwith\na newline",
      "\u001dleading-sentinel",
      "🔥"
    ]
    const instanceKeys = names.map((name) => ReplicaDefinition.documentInstanceKey(name, documentId))
    const refreshKeys = names.map(ReplicaDefinition.documentTypeRefreshKey)

    assert.strictEqual(new Set(instanceKeys).size, names.length)
    assert.strictEqual(new Set(refreshKeys).size, names.length)
    assert.strictEqual(
      new Set([...instanceKeys, ...refreshKeys]).size,
      instanceKeys.length + refreshKeys.length
    )
  })

  it("reports query and type refresh keys as full definition invalidation keys", () => {
    const definition = ReplicaDefinition.make({
      name: "tasks",
      documents: DocumentSet.make(Task),
      projections: [TaskRows]
    })
    assert.deepStrictEqual(ReplicaDefinition.invalidationKeys(definition), [
      "Task",
      "TaskRows",
      "[\"@lucas-barake/effect-local/invalidation\",\"document-type-refresh\",\"Task\"]"
    ])
  })

  it("deduplicates colliding raw query keys without dropping type refresh keys", () => {
    const SameNameRows = Projection.make("Task", {
      document: Task,
      version: 1,
      Row: Schema.Struct({ title: Schema.String }),
      key: (row) => row.title,
      project: (snapshot) => [{ title: snapshot.value.title }]
    })
    const definition = ReplicaDefinition.make({
      name: "tasks",
      documents: DocumentSet.make(Task),
      projections: [SameNameRows]
    })

    assert.deepStrictEqual(ReplicaDefinition.invalidationKeys(definition), [
      "Task",
      "[\"@lucas-barake/effect-local/invalidation\",\"document-type-refresh\",\"Task\"]"
    ])
  })

  it("changes the hash when a mutation is renamed", () => {
    const base = ReplicaDefinition.make({
      name: "tasks",
      documents: DocumentSet.make(Task),
      mutations: [Mutation.make("Rename", { document: Task, payload: Schema.String })]
    })
    const renamed = ReplicaDefinition.make({
      name: "tasks",
      documents: DocumentSet.make(Task),
      mutations: [Mutation.make("Renamed", { document: Task, payload: Schema.String })]
    })
    assert.notStrictEqual(base.hash, renamed.hash)
  })

  it("rejects empty names, foreign projection and query references, and duplicate collections", () => {
    const ForeignDoc = Document.make("Foreign", { schema: Schema.String, version: 1 })
    const ForeignProjection = Projection.make("Foreign", {
      document: ForeignDoc,
      version: 1,
      Row: Schema.Struct({ x: Schema.String }),
      key: (row) => row.x,
      project: () => []
    })
    assert.throws(() => ReplicaDefinition.make({ name: "", documents: DocumentSet.make(Task) }))
    assert.throws(() =>
      ReplicaDefinition.make({
        name: "tasks",
        documents: DocumentSet.make(Task),
        projections: [ForeignProjection]
      })
    )
    assert.throws(() =>
      ReplicaDefinition.make({
        name: "tasks",
        documents: DocumentSet.make(Task),
        queries: [ListTasks]
      })
    )
    assert.throws(() =>
      ReplicaDefinition.make({
        name: "tasks",
        documents: DocumentSet.make(Task),
        projections: [TaskRows, TaskRows]
      })
    )
    assert.throws(() =>
      ReplicaDefinition.make({
        name: "tasks",
        documents: DocumentSet.make(Task),
        projections: [TaskRows],
        queries: [ListTasks, ListTasks]
      })
    )
  })
})
