import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as DocumentSet from "../src/DocumentSet.js"
import * as Mutation from "../src/Mutation.js"
import * as Projection from "../src/Projection.js"
import * as Query from "../src/Query.js"
import * as ReplicaDefinition from "../src/ReplicaDefinition.js"

describe("ReplicaDefinition", () => {
  const makeFixture = (version = 1) => {
    const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version })
    const Rename = Mutation.make("Rename", { document: Task, payload: Schema.String })
    const TaskRows = Projection.make("TaskRows", {
      document: Task,
      version: 1,
      Row: Schema.Struct({ title: Schema.String }),
      key: (row) => row.title,
      project: (snapshot) => [{ title: snapshot.value.title }]
    })
    const ListTasks = Query.make("ListTasks", {
      success: Schema.Array(TaskRows.Row),
      dependsOn: [TaskRows]
    })
    return { Task, Rename, TaskRows, ListTasks }
  }

  it("builds compatible definitions with a stable hash", () => {
    const first = makeFixture()
    const second = makeFixture()
    const a = ReplicaDefinition.make({
      name: "tasks",
      documents: DocumentSet.make(first.Task),
      mutations: [first.Rename],
      projections: [first.TaskRows],
      queries: [first.ListTasks]
    })
    const b = ReplicaDefinition.make({
      name: "tasks",
      documents: DocumentSet.make(second.Task),
      mutations: [second.Rename],
      projections: [second.TaskRows],
      queries: [second.ListTasks]
    })
    assert.strictEqual(a.hash, b.hash)
    assert.notStrictEqual(
      a.hash,
      ReplicaDefinition.make({
        name: "tasks",
        documents: DocumentSet.make(makeFixture(2).Task),
        mutations: [],
        projections: [],
        queries: []
      }).hash
    )
  })

  it("defaults omitted collections to empty tuples", () => {
    const fixture = makeFixture()
    const minimal = ReplicaDefinition.make({
      name: "tasks",
      documents: DocumentSet.make(fixture.Task)
    })
    const explicit = ReplicaDefinition.make({
      name: "tasks",
      documents: DocumentSet.make(fixture.Task),
      mutations: [],
      projections: [],
      queries: []
    })
    const mutations: readonly [] = minimal.mutations
    const projections: readonly [] = minimal.projections
    const queries: readonly [] = minimal.queries
    assert.deepStrictEqual(mutations, [])
    assert.deepStrictEqual(projections, [])
    assert.deepStrictEqual(queries, [])
    assert.strictEqual(minimal.hash, explicit.hash)
  })

  it("rejects foreign references and duplicate names", () => {
    const fixture = makeFixture()
    const Foreign = Document.make("Foreign", { schema: Schema.String, version: 1 })
    const ForeignMutation = Mutation.make("Rename", { document: Foreign })
    assert.throws(() =>
      ReplicaDefinition.make({
        name: "tasks",
        documents: DocumentSet.make(fixture.Task),
        mutations: [ForeignMutation],
        projections: [],
        queries: []
      })
    )
    assert.throws(() =>
      ReplicaDefinition.make({
        name: "tasks",
        documents: DocumentSet.make(fixture.Task),
        mutations: [fixture.Rename, fixture.Rename],
        projections: [fixture.TaskRows],
        queries: [fixture.ListTasks]
      })
    )
  })
})
