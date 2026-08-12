import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Field from "@lucas-barake/effect-local/Field"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"

const TodoSchema = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  count: Schema.Number,
  labels: Schema.Array(Schema.String)
})
export const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: TodoSchema,
  indexes: {
    byCount: {
      version: 1,
      partition: [],
      sort: [{
        name: "count",
        affinity: "real",
        schema: Schema.Number,
        extract: (todo: typeof TodoSchema.Type) => todo.count
      }]
    }
  }
})

const MessageSchema = Schema.Struct({
  id: Schema.String,
  chatId: Schema.String,
  sentAt: Schema.Number,
  body: Schema.String
})
export const Message = Model.make("Message", {
  version: 1,
  key: Schema.String,
  schema: MessageSchema,
  indexes: {
    byChat: {
      version: 1,
      partition: [{
        name: "chatId",
        affinity: "text",
        schema: Schema.String,
        extract: (message: typeof MessageSchema.Type) => message.chatId
      }],
      sort: [{
        name: "sentAt",
        affinity: "real",
        schema: Schema.Number,
        extract: (message: typeof MessageSchema.Type) => message.sentAt
      }]
    }
  }
})

export const PutMessage = Mutation.make("PutMessage", {
  version: 1,
  payload: Message.schema
})

export const DeleteMessage = Mutation.make("DeleteMessage", {
  version: 1,
  payload: { id: Schema.String }
})

export const PutManyMessages = Mutation.make("PutManyMessages", {
  version: 1,
  payload: { count: Schema.Number, chats: Schema.Number }
})

export const PutTodo = Mutation.make("PutTodo", {
  version: 1,
  payload: Todo.schema,
  success: Todo.schema
})

export const RenameTodo = Mutation.make("RenameTodo", {
  version: 1,
  payload: { id: Schema.String, title: Schema.String },
  rejection: Schema.Literal("TodoNotFound")
})

export const DeleteTodo = Mutation.make("DeleteTodo", {
  version: 1,
  payload: { id: Schema.String }
})

export const PutManyTodos = Mutation.make("PutManyTodos", {
  version: 1,
  payload: { count: Schema.Number }
})

export const IncrementTodo = Mutation.make("IncrementTodo", {
  version: 1,
  payload: { id: Schema.String, delta: Schema.Number },
  rejection: Schema.Literal("TodoNotFound"),
  success: Schema.Number
})

export const AddLabel = Mutation.make("AddLabel", {
  version: 1,
  payload: { id: Schema.String, label: Schema.String },
  rejection: Schema.Literal("TodoNotFound"),
  success: Schema.Array(Schema.String)
})

export const RejectAfterWrite = Mutation.make("RejectAfterWrite", {
  version: 1,
  payload: Todo.schema,
  rejection: Schema.Literal("Rejected")
})

export const PutHugeTodo = Mutation.make("PutHugeTodo", {
  version: 1,
  payload: { id: Schema.String },
  success: Todo.schema
})

export const ReturnHugeResult = Mutation.make("ReturnHugeResult", {
  version: 1,
  success: Schema.String
})

const hugeTitle = "x".repeat(Protocol.maximumBatchBytes)

const ListTodos = Query.make("ListTodos", {
  success: Schema.Array(Todo.schema)
})

export const ReadCountIndex = Query.make("ReadCountIndex", {
  payload: { minimum: Schema.Number, direction: Schema.Literals(["asc", "desc"]) },
  success: Schema.Struct({
    first: Schema.Array(Schema.String),
    second: Schema.Array(Schema.String),
    streamed: Schema.Array(Schema.String)
  })
})

export const definition = Definition.make({
  version: 1,
  models: [Todo, Message],
  mutations: [
    PutMessage,
    DeleteMessage,
    PutManyMessages,
    PutTodo,
    RenameTodo,
    DeleteTodo,
    PutManyTodos,
    IncrementTodo,
    AddLabel,
    RejectAfterWrite,
    PutHugeTodo,
    ReturnHugeResult
  ],
  queries: [ListTodos, ReadCountIndex]
})

const getTodo = (transaction: Parameters<ReturnType<typeof RenameTodo.of>>[0]["transaction"], id: string) =>
  transaction.get(Todo, id).pipe(
    Effect.flatMap(Option.match({
      onNone: () => Effect.fail("TodoNotFound" as const),
      onSome: Effect.succeed
    }))
  )

export const handlers = Layer.mergeAll(
  PutMessage.toLayer(({ payload, transaction }) => transaction.set(Message, payload.id, payload)),
  DeleteMessage.toLayer(({ payload, transaction }) => transaction.delete(Message, payload.id)),
  PutManyMessages.toLayer(({ payload, transaction }) =>
    Effect.forEach(
      Array.from({ length: payload.count }, (_, index) => ({
        id: `bulk-${index}`,
        chatId: `chat-${index % payload.chats}`,
        sentAt: index,
        body: `body-${index}`
      })),
      (message) => transaction.set(Message, message.id, message),
      { discard: true }
    )
  ),
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))),
  RenameTodo.toLayer(({ payload, transaction }) =>
    Effect.gen(function*() {
      const current = yield* getTodo(transaction, payload.id)
      yield* transaction.set(Todo, payload.id, { ...current, title: payload.title })
    })
  ),
  DeleteTodo.toLayer(({ payload, transaction }) => transaction.delete(Todo, payload.id)),
  PutManyTodos.toLayer(({ payload, transaction }) =>
    Effect.forEach(
      Array.from({ length: payload.count }, (_, index) => todo(`bulk-${index}`)),
      (value) => transaction.set(Todo, value.id, value),
      { discard: true }
    )
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
  RejectAfterWrite.toLayer(({ payload, transaction }) =>
    transaction.set(Todo, payload.id, payload).pipe(Effect.andThen(Effect.fail("Rejected" as const)))
  ),
  PutHugeTodo.toLayer(({ payload, transaction }) => {
    const value = todo(payload.id, hugeTitle)
    return transaction.set(Todo, payload.id, value).pipe(Effect.as(value))
  }),
  ReturnHugeResult.toLayer(() => Effect.succeed(hugeTitle)),
  ListTodos.toLayer(({ query }) =>
    query.from(Todo, "byCount").stream().pipe(
      Stream.runCollect,
      Effect.map((items) => Array.from(items))
    )
  ),
  ReadCountIndex.toLayer(({ payload, query }) =>
    Effect.gen(function*() {
      const builder = query.from(Todo, "byCount")
        .where({ count: { gte: payload.minimum } })
        .order(payload.direction)
        .limit(2)
      const first = yield* builder.page()
      let second: ReadonlyArray<typeof Todo.schema.Type> = []
      if (first.next !== undefined) second = (yield* builder.after(first.next).page()).items
      const streamed = yield* builder.stream().pipe(Stream.runCollect)
      return {
        first: first.items.map((todo) => todo.id),
        second: second.map((todo) => todo.id),
        streamed: Array.from(streamed, (todo) => todo.id)
      }
    })
  )
)

export const todo = (id: string, title = "first") => ({ id, title, count: 0, labels: [] })
