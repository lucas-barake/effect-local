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

  it("is immune to post-make mutation of the caller's documents.byName map", () => {
    const fixture = makeFixture()
    const documents = DocumentSet.make(fixture.Task)
    const def = ReplicaDefinition.make({
      name: "tasks",
      documents,
      mutations: [fixture.Rename],
      projections: [fixture.TaskRows],
      queries: [fixture.ListTasks]
    })
    const hashBefore = def.hash
    const Extra = Document.make("Extra", { schema: Schema.String, version: 1 })
    ;(documents.byName as unknown as Map<string, Document.Any>).set("Extra", Extra)

    assert.strictEqual(DocumentSet.get(def.documents, "Extra"), undefined)
    assert.strictEqual(def.documents.byName.size, 1)
    assert.strictEqual(def.hash, hashBefore)
  })

  it("is immune to post-make mutation of a hand-rolled documents array", () => {
    const fixture = makeFixture()
    const documents: Array<Document.Any> = [fixture.Task]
    const def = ReplicaDefinition.make({
      name: "tasks",
      documents: { documents, byName: new Map([[fixture.Task.name, fixture.Task]]) },
      mutations: [fixture.Rename],
      projections: [fixture.TaskRows],
      queries: [fixture.ListTasks]
    })
    const hashBefore = def.hash
    const Extra = Document.make("Extra", { schema: Schema.String, version: 1 })
    documents.push(Extra)

    assert.strictEqual(def.documents.documents.length, 1)
    assert.deepStrictEqual([...def.documents.documents], [fixture.Task])
    assert.strictEqual(def.hash, hashBefore)
  })

  it("is immune to post-make mutation of the caller's arrays", () => {
    const fixture = makeFixture()
    const mutations: Array<Mutation.Any> = [fixture.Rename]
    const projections: Array<Projection.Any> = [fixture.TaskRows]
    const queries: Array<Query.Any> = [fixture.ListTasks]
    const documents = DocumentSet.make(fixture.Task)
    const def = ReplicaDefinition.make({
      name: "tasks",
      documents,
      mutations,
      projections,
      queries
    })
    const hashBefore = def.hash

    const OtherTask = Document.make("OtherTask", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
    const Delete = Mutation.make("Delete", { document: fixture.Task })
    const OtherProjection = Projection.make("OtherRows", {
      document: fixture.Task,
      version: 1,
      Row: Schema.Struct({ title: Schema.String }),
      key: (row) => row.title,
      project: (snapshot) => [{ title: snapshot.value.title }]
    })
    const OtherQuery = Query.make("OtherQuery", { success: Schema.Array(fixture.TaskRows.Row), dependsOn: [] })

    mutations.push(Delete)
    projections.push(OtherProjection)
    queries.push(OtherQuery)
    assert.throws(() => (documents.documents as unknown as Array<Document.Any>).push(OtherTask))

    assert.strictEqual(def.mutations.length, 1)
    assert.strictEqual(def.projections.length, 1)
    assert.strictEqual(def.queries.length, 1)
    assert.strictEqual(def.documents.documents.length, 1)
    assert.strictEqual(def.hash, hashBefore)
    assert.deepStrictEqual(def.mutations, [fixture.Rename])

    assert.throws(() => (def.mutations as Array<Mutation.Any>).push(Delete))
    assert.throws(() => (def.projections as Array<Projection.Any>).push(OtherProjection))
    assert.throws(() => (def.queries as Array<Query.Any>).push(OtherQuery))
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
