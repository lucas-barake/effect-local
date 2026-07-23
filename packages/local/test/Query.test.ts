import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as Projection from "../src/Projection.js"
import * as Query from "../src/Query.js"

describe("Query", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const TaskRows = Projection.make("TaskRows", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ title: Schema.String }),
    key: (row) => row.title,
    project: (snapshot) => [{ title: snapshot.value.title }]
  })
  const ListTasks = Query.make("ListTasks", {
    payload: Schema.String,
    success: Schema.Array(TaskRows.Row),
    dependsOn: [TaskRows]
  })

  it("defaults errors and normalizes dependencies", () => {
    assert.strictEqual(ListTasks.errorSchema, Schema.Never)
    assert.deepStrictEqual(ListTasks.dependsOn, [TaskRows])
    assert.throws(() =>
      Query.make("DuplicateDependencies", {
        payload: Schema.Void,
        success: Schema.Void,
        dependsOn: [TaskRows, TaskRows]
      })
    )
  })

  it("is immune to post-make mutation of the caller's dependsOn array", () => {
    const dependsOn: Array<Projection.Any> = [TaskRows]
    const query = Query.make("Immutable", { success: Schema.Array(TaskRows.Row), dependsOn })
    const OtherProjection = Projection.make("OtherRows", {
      document: Task,
      version: 1,
      Row: Schema.Struct({ title: Schema.String }),
      key: (row) => row.title,
      project: (snapshot) => [{ title: snapshot.value.title }]
    })
    dependsOn.push(OtherProjection)
    assert.strictEqual(query.dependsOn.length, 1)
    assert.deepStrictEqual(query.dependsOn, [TaskRows])
    assert.throws(() => (query.dependsOn as Array<Projection.Any>).push(OtherProjection))
  })

  it.effect("provides its effectful handler", () =>
    Effect.gen(function*() {
      const handler = yield* ListTasks.handler
      assert.deepStrictEqual(yield* handler("one"), [{ title: "one" }])
    }).pipe(Effect.provide(ListTasks.toLayer((title) => Effect.succeed([{ title }])))))

  it.effect("builds a handler effectfully", () =>
    Effect.gen(function*() {
      const handler = yield* ListTasks.handler
      assert.deepStrictEqual(yield* handler("one"), [{ title: "ONE" }])
    }).pipe(
      Effect.provide(
        ListTasks.toLayer(Effect.succeed((title) => Effect.succeed([{ title: title.toUpperCase() }])))
      )
    ))

  it.effect("releases per-call resources when the call scope closes", () =>
    Effect.gen(function*() {
      const released = yield* Ref.make(false)
      const ScopedQuery = Query.make("ScopedQuery", { dependsOn: [] })
      yield* Effect.gen(function*() {
        const handler = yield* ScopedQuery.handler
        yield* Effect.scoped(handler(undefined))
        assert.isTrue(yield* Ref.get(released))
      }).pipe(
        Effect.provide(
          ScopedQuery.toLayer(() =>
            Effect.acquireRelease(
              Effect.void,
              () => Ref.set(released, true)
            )
          )
        )
      )
    }))

  it.effect("keeps same-name handler services independent", () => {
    const OtherListTasks = Query.make("ListTasks", {
      payload: Schema.String,
      success: Schema.Array(TaskRows.Row),
      dependsOn: [TaskRows]
    })
    assert.notStrictEqual(ListTasks.handler.key, OtherListTasks.handler.key)
    return Effect.gen(function*() {
      const first = yield* ListTasks.handler
      const second = yield* OtherListTasks.handler
      assert.deepStrictEqual(yield* first("one"), [{ title: "one" }])
      assert.deepStrictEqual(yield* second("one"), [{ title: "ONE" }])
    }).pipe(Effect.provide(Layer.merge(
      ListTasks.toLayer((title) => Effect.succeed([{ title }])),
      OtherListTasks.toLayer((title) => Effect.succeed([{ title: title.toUpperCase() }]))
    )))
  })
})
