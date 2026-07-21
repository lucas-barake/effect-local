import * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"

export const Heads = Schema.Array(Schema.String)
export type Heads = typeof Heads.Type

export const Commit = Schema.Struct({
  commandId: Identity.CommandId,
  documentId: Identity.DocumentId,
  heads: Heads,
  sequence: Identity.CommitSequence
})
export type Commit = typeof Commit.Type
