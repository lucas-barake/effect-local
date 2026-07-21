import type * as Document from "./Document.js"
import type * as Identity from "./Identity.js"

export type ProjectionState = "Ready" | "Blocked" | "Rebuilding"

export interface Snapshot<A,> {
  readonly documentId: Identity.DocumentId
  readonly value: A
  readonly version: number
  readonly heads: ReadonlyArray<string>
  readonly tombstone: boolean
  readonly projection: ProjectionState
}

export type FromDocument<D extends Document.Any,> = Snapshot<D["schema"]["Type"]>
