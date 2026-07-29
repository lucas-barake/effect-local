import * as Document from "@lucas-barake/effect-local/Document"
import * as Schema from "effect/Schema"

const Title = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160))

const TaskV1 = Schema.Struct({
  title: Title,
  completed: Schema.Boolean,
  createdAt: Schema.Number,
  updatedAt: Schema.Number
})

export const TaskDocumentV2 = Document.make("Task", {
  schema: Schema.Struct({
    title: Title,
    completed: Schema.Boolean,
    priority: Schema.Int,
    createdAt: Schema.Number,
    updatedAt: Schema.Number
  }),
  version: 2,
  migrations: [
    Document.migration({
      from: 1,
      schema: TaskV1,
      migrate: (value) => ({ ...value, priority: 0 })
    })
  ]
})
