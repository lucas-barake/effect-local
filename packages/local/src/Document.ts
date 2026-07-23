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

export interface Migration<S extends DocumentSchema, out Out = unknown,> {
  readonly from: number
  readonly schema: S
  readonly migrate: (value: S["Type"]) => Out
}

export interface AnyMigration {
  readonly from: number
  readonly schema: DocumentSchema
  readonly migrate: (value: any) => unknown
}

export interface Document<out Name extends string, out S extends DocumentSchema,> {
  readonly name: Name
  readonly schema: S
  readonly version: number
  readonly migrations: ReadonlyArray<AnyMigration>
}

export type Any = Document<string, DocumentSchema>

export const migration = <S extends DocumentSchema, Out = unknown,>(options: {
  readonly from: number
  readonly schema: S
  readonly migrate: (value: S["Type"]) => Out
}): Migration<S, Out> => {
  if (!Number.isSafeInteger(options.from) || options.from < 1) {
    throw new TypeError("Migration source version must be a positive integer")
  }
  return { from: options.from, schema: options.schema, migrate: options.migrate }
}

export const make = <const Name extends string, S extends DocumentSchema,>(
  name: Name,
  options: {
    readonly schema: S
    readonly version: number
    readonly migrations?: ReadonlyArray<AnyMigration>
  }
): Document<Name, S> => {
  if (name.length === 0) throw new TypeError("Document name must be nonempty")
  if (!Number.isSafeInteger(options.version) || options.version < 1) {
    throw new TypeError("Document version must be a positive integer")
  }
  const migrations = options.migrations ?? []
  const sources = new Set<number>()
  for (const step of migrations) {
    if (step.from >= options.version) {
      throw new TypeError(`Migration source version must be below the document version: ${step.from}`)
    }
    if (sources.has(step.from)) throw new TypeError(`Duplicate migration source version: ${step.from}`)
    sources.add(step.from)
  }
  if (migrations.length > 0) {
    const oldest = Math.min(...sources)
    for (let version = oldest; version < options.version; version++) {
      if (!sources.has(version)) {
        throw new TypeError(`Migration chain has a gap at version ${version}`)
      }
    }
  }
  return { name, schema: options.schema, version: options.version, migrations }
}

export const supportsStoredVersion = (self: Any, storedVersion: number): boolean => {
  if (storedVersion === self.version) return true
  if (storedVersion > self.version || storedVersion < 1) return false
  for (let version = storedVersion; version < self.version; version++) {
    if (!self.migrations.some((step) => step.from === version)) return false
  }
  return true
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
          current instanceof Date || current instanceof Uint8Array || Automerge.isCounter(current) ||
          Automerge.isImmutableString(current)
        ) return true
        if (Automerge.isAutomerge(current)) return false
        if (seen.has(current)) return false
        seen.add(current)
        if (Array.isArray(current)) {
          for (let index = 0; index < current.length; index++) {
            if (!visit(current[index])) return false
          }
          return true
        }
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
        reason: new ReplicaError.DocumentDecodeError({
          documentId,
          cause
        })
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
        reason: new ReplicaError.DocumentEncodeError({
          documentId,
          cause
        })
      })
    )
  )

const decodeFailure = (documentId: Identity.DocumentId, cause: unknown) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.DocumentDecodeError({
      documentId,
      cause
    })
  })

export const decodeStored = <Name extends string, S extends DocumentSchema,>(
  self: Document<Name, S>,
  documentId: Identity.DocumentId,
  storedVersion: number,
  input: unknown
): Effect.Effect<S["Type"], ReplicaError.ReplicaError> => {
  if (storedVersion === self.version) return decode(self, documentId, input)
  const steps: Array<AnyMigration> = []
  for (let version = storedVersion; version < self.version; version++) {
    const step = self.migrations.find((candidate) => candidate.from === version)
    if (step === undefined) {
      return new ReplicaError.ReplicaError({
        reason: new ReplicaError.UnsupportedDocumentVersion({
          documentId,
          observedVersion: storedVersion,
          supportedVersion: self.version
        })
      })
    }
    steps.push(step)
  }
  if (steps.length === 0) {
    return new ReplicaError.ReplicaError({
      reason: new ReplicaError.UnsupportedDocumentVersion({
        documentId,
        observedVersion: storedVersion,
        supportedVersion: self.version
      })
    })
  }
  return Effect.gen(function*() {
    let value: unknown = yield* Schema.decodeUnknownEffect(steps[0]!.schema)(input).pipe(
      Effect.mapError((cause) => decodeFailure(documentId, cause))
    )
    for (let index = 0; index < steps.length; index++) {
      const step = steps[index]!
      value = yield* Effect.try({
        try: () => step.migrate(value),
        catch: (cause) => decodeFailure(documentId, cause)
      })
      if (index + 1 < steps.length) {
        value = yield* Schema.decodeUnknownEffect(Schema.toType(steps[index + 1]!.schema))(value).pipe(
          Effect.mapError((cause) => decodeFailure(documentId, cause))
        )
      }
    }
    return yield* Schema.decodeUnknownEffect(Schema.toType(self.schema))(value).pipe(
      Effect.mapError((cause) => decodeFailure(documentId, cause))
    )
  })
}
