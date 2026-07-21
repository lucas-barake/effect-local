import * as Automerge from "@automerge/automerge"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Identity from "./Identity.js"
import * as ReplicaError from "./ReplicaError.js"

export type WireSchema = Schema.Codec<unknown, unknown, never, never>
export type AutomergeEncoded = Automerge.ScalarValue | {
  readonly [key: string]: AutomergeEncoded
} | ReadonlyArray<AutomergeEncoded>
export type DocumentSchema = Schema.Codec<unknown, AutomergeEncoded, never, never>

export interface Document<out Name extends string, out S extends DocumentSchema,> {
  readonly name: Name
  readonly schema: S
  readonly version: number
}

export type Any = Document<string, DocumentSchema>

export const make = <const Name extends string, S extends DocumentSchema,>(
  name: Name,
  options: { readonly schema: S; readonly version: number }
): Document<Name, S> => {
  if (name.length === 0) throw new TypeError("Document name must be nonempty")
  if (!Number.isSafeInteger(options.version) || options.version < 1) {
    throw new TypeError("Document version must be a positive integer")
  }
  return { name, schema: options.schema, version: options.version }
}

export const isAutomergeValue = (value: unknown): boolean => {
  const seen = new WeakSet<object>()
  const visit = (current: unknown): boolean => {
    if (current === null) return true
    switch (typeof current) {
      case "string":
      case "boolean":
        return true
      case "number":
        return Number.isFinite(current)
      case "object": {
        if (
          current instanceof Date || current instanceof Uint8Array || current instanceof Automerge.Counter ||
          current instanceof Automerge.ImmutableString
        ) return true
        if (Automerge.isAutomerge(current)) return false
        if (seen.has(current)) return false
        seen.add(current)
        if (Array.isArray(current)) return current.every(visit)
        if (Object.getPrototypeOf(current) !== Object.prototype && Object.getPrototypeOf(current) !== null) return false
        return Object.values(current).every(visit)
      }
      default:
        return false
    }
  }
  return visit(value)
}

export const decode = <Name extends string, S extends DocumentSchema,>(
  self: Document<Name, S>,
  documentId: Identity.DocumentId,
  input: unknown
) =>
  Schema.decodeUnknownEffect(self.schema)(input).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: {
          _tag: "DocumentDecodeError",
          documentId,
          cause: { _tag: "SchemaCause", message: String(cause), path: [] }
        }
      })
    )
  )

export const encode = <Name extends string, S extends DocumentSchema,>(
  self: Document<Name, S>,
  documentId: Identity.DocumentId,
  value: S["Type"]
) =>
  Schema.encodeEffect(self.schema)(value).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: {
          _tag: "DocumentDecodeError",
          documentId,
          cause: { _tag: "SchemaCause", message: String(cause), path: [] }
        }
      })
    )
  )
