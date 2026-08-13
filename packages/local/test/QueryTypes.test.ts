import { assert, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Model from "../src/Model.js"
import * as Mutation from "../src/Mutation.js"
import * as Query from "../src/Query.js"
import type * as SecondaryIndex from "../src/SecondaryIndex.js"
import type * as Transaction from "../src/Transaction.js"

const TodoSchema = Schema.Struct({
  id: Schema.String,
  projectId: Schema.String,
  createdAt: Schema.Number,
  priority: Schema.Number
})

const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: TodoSchema,
  indexes: {
    byProjectCreatedAt: {
      version: 1,
      partition: [{
        name: "projectId",
        affinity: "text",
        schema: Schema.String,
        extract: (todo: typeof TodoSchema.Type) => todo.projectId
      }],
      sort: [{
        name: "createdAt",
        affinity: "real",
        schema: Schema.Number,
        extract: (todo: typeof TodoSchema.Type) => todo.createdAt
      }]
    },
    byPriority: {
      version: 1,
      partition: [],
      sort: [{
        name: "priority",
        affinity: "integer",
        schema: Schema.Int,
        extract: (todo: typeof TodoSchema.Type) => todo.priority
      }]
    }
  }
})

const typeContracts = (
  query: Transaction.Query,
  createdCursor: SecondaryIndex.Cursor<"Todo", "byProjectCreatedAt">,
  priorityCursor: SecondaryIndex.Cursor<"Todo", "byPriority">
) => {
  const created = query.from(Todo, "byProjectCreatedAt")
    .where({ projectId: "project-1", createdAt: { gte: 10, lt: 20 } })
    .order("desc")
    .limit(25)
    .after(createdCursor)
  created.page()
  created.stream()

  query.from(Todo, "byPriority").where({ priority: { gt: 1 } }).after(priorityCursor)

  // @ts-expect-error unindexed model scans are not part of the query capability
  query.all(Todo)

  // @ts-expect-error undeclared indexes cannot be queried
  query.from(Todo, "byTitle")
  // @ts-expect-error where fields must belong to the declared index
  query.from(Todo, "byProjectCreatedAt").where({ projectId: "project-1", priority: { gt: 1 } })
  // @ts-expect-error partition values retain their declared Schema type
  query.from(Todo, "byProjectCreatedAt").where({ projectId: 1 })
  // @ts-expect-error cursors are scoped to their model and index
  query.from(Todo, "byProjectCreatedAt").after(priorityCursor)
}

// @ts-expect-error static dependency fallbacks are not part of query declarations
Query.make("LegacyDependencies", { dependsOn: [Todo] })

class DeclaredFailure extends Schema.TaggedErrorClass<DeclaredFailure>(
  "@lucas-barake/effect-local/test/DeclaredFailure"
)("DeclaredFailure", {}) {}

Query.make("TaggedFailure", { error: DeclaredFailure })
Mutation.make("TaggedRejection", { version: 1, rejection: DeclaredFailure })

// @ts-expect-error query errors must have a required _tag
Query.make("UntaggedFailure", { error: Schema.String })
// @ts-expect-error mutation rejections must have a required _tag
Mutation.make("UntaggedRejection", { version: 1, rejection: Schema.String })

void typeContracts

it("preserves declared secondary index names at runtime", () => {
  const indexNames = Object.keys(Todo.indexes)
  assert.deepStrictEqual(indexNames, ["byProjectCreatedAt", "byPriority"])
})
