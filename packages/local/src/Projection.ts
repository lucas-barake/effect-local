import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Document from "./Document.js"
import * as ReplicaError from "./ReplicaError.js"
import type * as Snapshot from "./Snapshot.js"

export interface Projection<
  out Name extends string,
  D extends Document.Any,
  R extends Document.WireSchema,
> {
  readonly name: Name
  readonly document: D
  readonly version: number
  readonly Row: R
  readonly key: (row: R["Type"]) => string
  readonly project: (snapshot: Snapshot.FromDocument<D>) => ReadonlyArray<R["Type"]>
}

export type Any = Projection<any, any, any>

export const make = <const Name extends string, D extends Document.Any, R extends Document.WireSchema,>(
  name: Name,
  options: {
    readonly document: D
    readonly version: number
    readonly Row: R
    readonly key: (row: R["Type"]) => string
    readonly project: (snapshot: Snapshot.FromDocument<D>) => ReadonlyArray<R["Type"]>
  }
): Projection<Name, D, R> => {
  if (name.length === 0) throw new TypeError("Projection name must be nonempty")
  if (!Number.isSafeInteger(options.version) || options.version < 1) {
    throw new TypeError("Projection version must be a positive integer")
  }
  return { name, ...options }
}

export const assertUniqueKeys = <P extends Any,>(self: P, rows: ReadonlyArray<P["Row"]["Type"]>): void => {
  const keys = new Set<string>()
  for (const row of rows) {
    const key = self.key(row)
    if (keys.has(key)) throw new TypeError(`Duplicate projection key: ${key}`)
    keys.add(key)
  }
}

export const evaluate = <P extends Any,>(
  self: P,
  snapshot: Snapshot.FromDocument<P["document"]>
): Effect.Effect<ReadonlyArray<P["Row"]["Type"]>, ReplicaError.ReplicaError> =>
  Effect.try({
    try: () => {
      const validate = Schema.decodeUnknownSync(Schema.toType(self.Row))
      const rows = self.project(snapshot).map((row) => validate(row))
      assertUniqueKeys(self, rows)
      return rows
    },
    catch: (cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProjectionBlocked({
          projection: self.name,
          cause: new ReplicaError.SchemaCause({
            message: String(cause),
            path: []
          })
        })
      })
  })
