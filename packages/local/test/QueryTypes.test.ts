import { assert, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Model from "../src/Model.js"
import * as Mutation from "../src/Mutation.js"
import * as Query from "../src/Query.js"
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

const typeContracts = (query: Transaction.Query) => {
  query.sql([Todo], (sql) => sql`SELECT "id" FROM "Todo" WHERE "priority" > ${1}`)

  // @ts-expect-error the statement callback must return a statement, not raw text
  query.sql([Todo], () => "SELECT 1")
  // @ts-expect-error declared reads must be models
  query.sql(["Todo"], (sql) => sql`SELECT 1`)
  // @ts-expect-error the declared-index builder is gone; raw SQL replaced it
  query.from(Todo, "byPriority")
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
  assert.deepStrictEqual(Object.keys(Todo.indexes), ["byProjectCreatedAt", "byPriority"])
})
