import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Field from "@lucas-barake/effect-local/Field"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

export const Todo = Model.make("Todo", {
  key: Schema.String,
  schema: Schema.Struct({
    id: Schema.String,
    title: Schema.String,
    count: Schema.Number,
    labels: Schema.Array(Schema.String)
  })
})

export const PutTodo = Mutation.make("PutTodo", {
  payload: Todo.schema,
  success: Todo.schema
})

export const RenameTodo = Mutation.make("RenameTodo", {
  payload: { id: Schema.String, title: Schema.String },
  rejection: Schema.Literal("TodoNotFound")
})

export const IncrementTodo = Mutation.make("IncrementTodo", {
  payload: { id: Schema.String, delta: Schema.Number },
  rejection: Schema.Literal("TodoNotFound"),
  success: Schema.Number
})

export const AddLabel = Mutation.make("AddLabel", {
  payload: { id: Schema.String, label: Schema.String },
  rejection: Schema.Literal("TodoNotFound"),
  success: Schema.Array(Schema.String)
})

const ListTodos = Query.make("ListTodos", {
  success: Schema.Array(Todo.schema),
  dependsOn: [Todo]
})

export const definition = Definition.make({
  models: [Todo],
  mutations: [PutTodo, RenameTodo, IncrementTodo, AddLabel],
  queries: [ListTodos]
})

const getTodo = (transaction: Parameters<ReturnType<typeof RenameTodo.of>>[0]["transaction"], id: string) =>
  transaction.get(Todo, id).pipe(
    Effect.flatMap(Option.match({
      onNone: () => Effect.fail("TodoNotFound" as const),
      onSome: Effect.succeed
    }))
  )

export const handlers = Layer.mergeAll(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))),
  RenameTodo.toLayer(({ payload, transaction }) =>
    Effect.gen(function*() {
      const current = yield* getTodo(transaction, payload.id)
      yield* transaction.set(Todo, payload.id, { ...current, title: payload.title })
    })
  ),
  IncrementTodo.toLayer(({ payload, transaction }) =>
    Effect.gen(function*() {
      const current = yield* getTodo(transaction, payload.id)
      const count = yield* transaction.applyField(Field.counter, current.count, {
        _tag: "Increment",
        delta: payload.delta
      })
      yield* transaction.set(Todo, payload.id, { ...current, count })
      return count
    })
  ),
  AddLabel.toLayer(({ payload, transaction }) =>
    Effect.gen(function*() {
      const current = yield* getTodo(transaction, payload.id)
      const labels = yield* transaction.applyField(Field.growOnlySet(Schema.String), current.labels, {
        _tag: "Add",
        value: payload.label
      })
      yield* transaction.set(Todo, payload.id, { ...current, labels })
      return labels
    })
  ),
  ListTodos.toLayer(({ query }) => query.all(Todo))
)

export const todo = (id: string, title = "first") => ({ id, title, count: 0, labels: [] })
