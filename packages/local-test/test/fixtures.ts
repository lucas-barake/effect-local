import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Schema from "effect/Schema"

export const Task = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String }),
  version: 1
})

export const Rename = Mutation.make("Rename", {
  document: Task,
  payload: Schema.String
})

export const definition = ReplicaDefinition.make({
  name: "test-replica",
  documents: DocumentSet.make(Task),
  mutations: [Rename],
  projections: [],
  queries: []
})
