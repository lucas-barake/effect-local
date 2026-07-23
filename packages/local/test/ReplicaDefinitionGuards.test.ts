import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as DocumentSet from "../src/DocumentSet.js"
import * as Mutation from "../src/Mutation.js"
import * as Projection from "../src/Projection.js"
import * as Query from "../src/Query.js"
import * as ReplicaDefinition from "../src/ReplicaDefinition.js"

describe("ReplicaDefinition guards", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const TaskRows = Projection.make("TaskRows", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ title: Schema.String }),
    key: (row) => row.title,
    project: (snapshot) => [{ title: snapshot.value.title }]
  })
  const ListTasks = Query.make("ListTasks", { success: Schema.Array(TaskRows.Row), dependsOn: [TaskRows] })

  it("reports document and projection names as invalidation keys", () => {
    const definition = ReplicaDefinition.make({
      name: "tasks",
      documents: DocumentSet.make(Task),
      projections: [TaskRows]
    })
    assert.deepStrictEqual(ReplicaDefinition.invalidationKeys(definition), ["Task", "TaskRows"])
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
