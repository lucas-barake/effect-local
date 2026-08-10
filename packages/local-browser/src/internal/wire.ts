import type * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import { maxConflictDepthHardLimit, maxConflictNodesHardLimit } from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"

export interface ConflictLimits {
  readonly maxConflictDepth: number
  readonly maxConflictNodes: number
  readonly maxConflictAlternatives: number
  readonly maxConflictPathSegments: number
  readonly maxConflictValueBytes: number
}

export type ConflictPreflight<A,> = (
  value: A,
  limits: ConflictLimits
) => { readonly _tag: string } | undefined

type PreflightResult =
  | { readonly _tag: "Success" }
  | { readonly _tag: "Failure"; readonly observed: string }

const runSyncSafe = <A,>(thunk: () => A, fallback: A): A => {
  const exit = Effect.runSyncExit(Effect.try({ try: thunk, catch: () => fallback }))
  if (Exit.isSuccess(exit)) return exit.value
  return fallback
}

type PendingValue = {
  readonly _tag: "Value"
  readonly value: unknown
  readonly depth: number
}

type PendingExit = {
  readonly _tag: "Exit"
  readonly value: object
}

type SnapshotContainer = Array<unknown> | { [key: string]: unknown }

type PendingSnapshotValue = {
  readonly _tag: "SnapshotValue"
  readonly value: unknown
  readonly depth: number
  readonly parent: SnapshotContainer
  readonly key: string | number
}

type PendingSnapshotExit = {
  readonly _tag: "SnapshotExit"
  readonly value: object
}

// Pending entries pair an array parent with a numeric key and an object parent with a string key.
// The queue cannot express that correlation, so the write narrows at the parent.
const setSnapshotEntry = (parent: SnapshotContainer, key: string | number, value: unknown): void => {
  if (Array.isArray(parent)) {
    if (typeof key === "number") parent[key] = value
    else parent[Number(key)] = value
    return
  }
  if (typeof key === "string") parent[key] = value
}

const unsafeKeys = new Set([
  ...Object.getOwnPropertyNames(Object.prototype),
  "prototype"
])

// Portable conflict values add at most three JSON containers per semantic level. This bound leaves
// room for the RPC envelope while keeping Effect Schema's recursive JSON guard below the JS stack.
const maxSchemaJsonDepth = maxConflictDepthHardLimit * 4
const maxSchemaJsonNodes = maxConflictNodesHardLimit * 4

const exceedsUtf8Limit = (value: string, limit: number): boolean => {
  if (value.length > limit) return true
  let bytes = 0
  for (let index = 0; index < value.length; index++) {
    const codeUnit = value.charCodeAt(index)
    if (codeUnit <= 0x7f) {
      bytes += 1
    } else if (codeUnit <= 0x7ff) {
      bytes += 2
    } else if (
      codeUnit >= 0xd800 &&
      codeUnit <= 0xdbff &&
      index + 1 < value.length &&
      value.charCodeAt(index + 1) >= 0xdc00 &&
      value.charCodeAt(index + 1) <= 0xdfff
    ) {
      bytes += 4
      index += 1
    } else {
      bytes += 3
    }
    if (bytes > limit) return true
  }
  return false
}

const preflightConflictJsonUnsafe = (
  input: unknown,
  limits: ConflictLimits,
  semanticLimits: boolean
): PreflightResult => {
  const active = new WeakSet<object>()
  const pending: Array<PendingValue | PendingExit> = [{ _tag: "Value", value: input, depth: 0 }]
  let nodes = 0
  let alternatives = 0

  while (pending.length > 0) {
    const next = pending.pop()!
    if (next._tag === "Exit") {
      active.delete(next.value)
      continue
    }

    nodes += 1
    if (next.depth > maxSchemaJsonDepth) {
      return { _tag: "Failure", observed: "conflict JSON exceeds the safe nesting limit" }
    }
    if (semanticLimits && nodes > limits.maxConflictNodes) {
      return { _tag: "Failure", observed: "conflict JSON exceeds the advertised node limit" }
    }
    if (semanticLimits && next.depth > limits.maxConflictDepth) {
      return { _tag: "Failure", observed: "conflict JSON exceeds the advertised depth limit" }
    }

    const value = next.value
    if (value === null) continue
    switch (typeof value) {
      case "boolean":
        continue
      case "number": {
        if (!Number.isFinite(value)) {
          return { _tag: "Failure", observed: "conflict JSON contains a nonfinite number" }
        }
        continue
      }
      case "string":
        continue
      case "object":
        break
      default:
        return { _tag: "Failure", observed: `conflict JSON contains ${typeof value}` }
    }

    if (active.has(value)) {
      return { _tag: "Failure", observed: "conflict JSON contains a cycle" }
    }
    active.add(value)
    pending.push({ _tag: "Exit", value })

    if (Array.isArray(value)) {
      const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length")
      if (
        lengthDescriptor === undefined ||
        !("value" in lengthDescriptor) ||
        !Number.isSafeInteger(lengthDescriptor.value) ||
        lengthDescriptor.value < 0
      ) {
        return { _tag: "Failure", observed: "conflict JSON contains an invalid array length" }
      }
      const length = Number(lengthDescriptor.value)
      if (semanticLimits && length > limits.maxConflictNodes - nodes) {
        return { _tag: "Failure", observed: "conflict JSON exceeds the advertised node limit" }
      }
      const keys = Reflect.ownKeys(value)
      if (
        keys.some((key) =>
          typeof key !== "string" ||
          (key !== "length" && (!/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= length))
        )
      ) {
        return { _tag: "Failure", observed: "conflict JSON contains unsupported array properties" }
      }
      for (let index = length - 1; index >= 0; index--) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index))
        if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
          return { _tag: "Failure", observed: "conflict JSON contains a sparse or accessor array entry" }
        }
        pending.push({ _tag: "Value", value: descriptor.value, depth: next.depth + 1 })
      }
      continue
    }

    const prototype = Object.getPrototypeOf(value)
    if (prototype !== Object.prototype && prototype !== null) {
      return { _tag: "Failure", observed: "conflict JSON contains an unsupported object prototype" }
    }
    const keys = Reflect.ownKeys(value)
    if (semanticLimits && keys.length > limits.maxConflictNodes - nodes) {
      return { _tag: "Failure", observed: "conflict JSON exceeds the advertised node limit" }
    }
    if (keys.some((key) => typeof key !== "string")) {
      return { _tag: "Failure", observed: "conflict JSON contains a symbol key" }
    }
    const stringKeys = keys.filter((key): key is string => typeof key === "string")
    const data = Array.from<unknown>({ length: stringKeys.length })
    for (let index = 0; index < stringKeys.length; index++) {
      const key = stringKeys[index]
      if (key === undefined) return { _tag: "Failure", observed: "conflict JSON contains a missing key" }
      if (unsafeKeys.has(key)) {
        return { _tag: "Failure", observed: "conflict JSON contains a prototype sensitive key" }
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key)
      if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
        return { _tag: "Failure", observed: "conflict JSON contains an accessor or hidden property" }
      }
      data[index] = descriptor.value
    }
    const alternativesIndex = stringKeys.indexOf("alternatives")
    if (
      semanticLimits &&
      alternativesIndex !== -1 &&
      Array.isArray(data[alternativesIndex])
    ) {
      alternatives += data[alternativesIndex].length
      if (alternatives > limits.maxConflictAlternatives) {
        return { _tag: "Failure", observed: "conflict JSON exceeds the advertised alternative limit" }
      }
    }
    const parents = stringKeys.indexOf("parents")
    const target = stringKeys.indexOf("target")
    if (
      semanticLimits &&
      parents !== -1 &&
      target !== -1 &&
      Array.isArray(data[parents]) &&
      data[parents].length + 1 > limits.maxConflictPathSegments
    ) {
      return { _tag: "Failure", observed: "conflict JSON exceeds the advertised path limit" }
    }
    for (let index = data.length - 1; index >= 0; index--) {
      pending.push({ _tag: "Value", value: data[index], depth: next.depth + 1 })
    }
  }

  return { _tag: "Success" }
}

const preflightConflictJson = (
  input: unknown,
  limits: ConflictLimits,
  semanticLimits: boolean
): PreflightResult =>
  runSyncSafe(
    () => preflightConflictJsonUnsafe(input, limits, semanticLimits),
    { _tag: "Failure", observed: "conflict JSON inspection failed" }
  )

type SchemaSnapshotResult =
  | { readonly _tag: "Success"; readonly value: unknown }
  | { readonly _tag: "Failure"; readonly observed: string }

const snapshotSchemaInputUnsafe = (input: unknown): SchemaSnapshotResult => {
  const active = new WeakSet<object>()
  const root: { [key: string]: unknown } = { value: undefined }
  const pending: Array<PendingSnapshotValue | PendingSnapshotExit> = [{
    _tag: "SnapshotValue",
    value: input,
    depth: 0,
    parent: root,
    key: "value"
  }]
  let nodes = 0

  while (pending.length > 0) {
    const next = pending.pop()!
    if (next._tag === "SnapshotExit") {
      active.delete(next.value)
      continue
    }
    nodes += 1
    if (next.depth > maxSchemaJsonDepth) {
      return { _tag: "Failure", observed: "conflict value exceeds the safe schema nesting limit" }
    }
    if (nodes > maxSchemaJsonNodes) {
      return { _tag: "Failure", observed: "conflict value exceeds the safe schema node limit" }
    }
    if (
      (typeof next.value !== "object" && typeof next.value !== "function") ||
      next.value === null
    ) {
      setSnapshotEntry(next.parent, next.key, next.value)
      continue
    }
    if (active.has(next.value)) {
      return { _tag: "Failure", observed: "conflict value contains a cycle" }
    }
    if (!Array.isArray(next.value)) {
      const prototype = Object.getPrototypeOf(next.value)
      if (prototype !== Object.prototype && prototype !== null) {
        setSnapshotEntry(next.parent, next.key, next.value)
        continue
      }
    }
    active.add(next.value)
    pending.push({ _tag: "SnapshotExit", value: next.value })

    if (Array.isArray(next.value)) {
      const lengthDescriptor = Object.getOwnPropertyDescriptor(next.value, "length")
      if (
        lengthDescriptor === undefined ||
        !("value" in lengthDescriptor) ||
        !Number.isSafeInteger(lengthDescriptor.value) ||
        lengthDescriptor.value < 0
      ) {
        return { _tag: "Failure", observed: "conflict value contains an invalid array length" }
      }
      const length = Number(lengthDescriptor.value)
      if (length > maxSchemaJsonNodes - nodes) {
        return { _tag: "Failure", observed: "conflict value exceeds the safe schema node limit" }
      }
      const keys = Reflect.ownKeys(next.value)
      if (
        keys.some((key) =>
          typeof key !== "string" ||
          (key !== "length" && (!/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= length))
        )
      ) {
        return { _tag: "Failure", observed: "conflict value contains unsupported array properties" }
      }
      const output = Array.from<unknown>({ length })
      setSnapshotEntry(next.parent, next.key, output)
      for (let index = length - 1; index >= 0; index--) {
        const descriptor = Object.getOwnPropertyDescriptor(next.value, String(index))
        if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
          return { _tag: "Failure", observed: "conflict value contains a sparse or accessor array entry" }
        }
        pending.push({
          _tag: "SnapshotValue",
          value: descriptor.value,
          depth: next.depth + 1,
          parent: output,
          key: index
        })
      }
      continue
    }

    const keys = Reflect.ownKeys(next.value)
    if (keys.length > maxSchemaJsonNodes - nodes) {
      return { _tag: "Failure", observed: "conflict value exceeds the safe schema node limit" }
    }
    if (keys.some((key) => typeof key !== "string")) {
      return { _tag: "Failure", observed: "conflict value contains a symbol key" }
    }
    let output: { [key: string]: unknown } = {}
    if (Object.getPrototypeOf(next.value) === null) output = Object.create(null)
    setSnapshotEntry(next.parent, next.key, output)
    const stringKeys = keys.filter((key): key is string => typeof key === "string")
    for (let index = stringKeys.length - 1; index >= 0; index--) {
      const key = stringKeys[index]
      if (key === undefined) return { _tag: "Failure", observed: "conflict value contains a missing key" }
      if (unsafeKeys.has(key)) {
        return { _tag: "Failure", observed: "conflict value contains a prototype sensitive key" }
      }
      const descriptor = Object.getOwnPropertyDescriptor(next.value, key)
      if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
        return { _tag: "Failure", observed: "conflict value contains an accessor or hidden property" }
      }
      pending.push({
        _tag: "SnapshotValue",
        value: descriptor.value,
        depth: next.depth + 1,
        parent: output,
        key
      })
    }
  }

  return { _tag: "Success", value: root.value }
}

const snapshotSchemaInput = (input: unknown): SchemaSnapshotResult =>
  runSyncSafe(
    () => snapshotSchemaInputUnsafe(input),
    { _tag: "Failure", observed: "conflict value inspection failed" }
  )

export const encodeConflictText = (
  value: unknown,
  limits: ConflictLimits
): Effect.Effect<string, ReplicaError.ReplicaError> => encodeConflictJsonText(value, limits, true)

const encodeConflictJsonText = (
  value: unknown,
  limits: ConflictLimits,
  semanticLimits: boolean
): Effect.Effect<string, ReplicaError.ReplicaError> =>
  Effect.suspend(() => {
    const preflight = preflightConflictJson(value, limits, semanticLimits)
    if (preflight._tag === "Failure") {
      return Effect.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "bounded conflict JSON text",
            observed: preflight.observed
          })
        })
      )
    }
    return Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(value).pipe(
      Effect.mapError((cause) =>
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "bounded conflict JSON text",
            observed: String(cause)
          })
        })
      ),
      Effect.flatMap((encoded) => {
        if (exceedsUtf8Limit(encoded, limits.maxConflictValueBytes)) {
          return Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "bounded conflict JSON text",
                observed: "conflict JSON exceeds the advertised byte limit"
              })
            })
          )
        }
        return Effect.succeed(encoded)
      })
    )
  })

export const decodeConflictText = (
  value: string,
  limits: ConflictLimits
): Effect.Effect<unknown, ReplicaError.ReplicaError> => decodeConflictJsonText(value, limits, true)

const decodeConflictJsonText = (
  value: string,
  limits: ConflictLimits,
  semanticLimits: boolean
): Effect.Effect<unknown, ReplicaError.ReplicaError> =>
  Effect.suspend(() => {
    if (exceedsUtf8Limit(value, limits.maxConflictValueBytes)) {
      return Effect.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "bounded conflict JSON text",
            observed: "conflict JSON exceeds the advertised byte limit"
          })
        })
      )
    }
    return Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(value).pipe(
      Effect.mapError((cause) =>
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "bounded conflict JSON text",
            observed: String(cause)
          })
        })
      ),
      Effect.flatMap((decoded) => {
        const preflight = preflightConflictJson(decoded, limits, semanticLimits)
        if (preflight._tag === "Success") return Effect.succeed(decoded)
        return Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "bounded conflict JSON text",
              observed: preflight.observed
            })
          })
        )
      })
    )
  })

export const encodeConflict = <S extends Document.WireSchema,>(
  schema: S,
  value: S["Type"],
  limits: ConflictLimits,
  preflight?: ConflictPreflight<S["Type"]>
): Effect.Effect<string, ReplicaError.ReplicaError> => {
  let validated: Effect.Effect<S["Type"], ReplicaError.ReplicaError>
  if (preflight === undefined) {
    validated = Effect.succeed(value)
  } else {
    validated = Effect.try({
      try: () => preflight(value, limits),
      catch: (cause) =>
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "bounded semantic conflict value",
            observed: String(cause)
          })
        })
    }).pipe(
      Effect.flatMap((issue) => {
        if (issue === undefined) return Effect.succeed(value)
        return Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "bounded semantic conflict value",
              observed: issue._tag
            })
          })
        )
      })
    )
  }
  return validated.pipe(
    Effect.flatMap((snapshotInput) => {
      const snapshot = snapshotSchemaInput(snapshotInput)
      if (snapshot._tag === "Success") return Effect.succeed(snapshot.value)
      return Effect.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "schema coded conflict JSON",
            observed: snapshot.observed
          })
        })
      )
    }),
    Effect.flatMap(Schema.encodeUnknownEffect(Schema.toCodecJson(schema))),
    Effect.mapError((cause) => {
      if (ReplicaError.isReplicaError(cause)) return cause
      return new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "schema coded conflict JSON",
          observed: String(cause)
        })
      })
    }),
    Effect.flatMap((encoded) => encodeConflictJsonText(encoded, limits, false))
  )
}

export const decodeConflict = <S extends Document.WireSchema,>(
  schema: S,
  value: string,
  limits: ConflictLimits,
  preflight?: ConflictPreflight<S["Type"]>
): Effect.Effect<S["Type"], ReplicaError.ReplicaError> =>
  decodeConflictJsonText(value, limits, false).pipe(
    Effect.flatMap((decoded) =>
      Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(decoded).pipe(
        Effect.mapError((cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "schema coded conflict JSON",
              observed: String(cause)
            })
          })
        )
      )
    ),
    Effect.flatMap((decoded) => {
      if (preflight === undefined) return Effect.succeed(decoded)
      return Effect.try({
        try: () => preflight(decoded, limits),
        catch: (cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "bounded semantic conflict value",
              observed: String(cause)
            })
          })
      }).pipe(
        Effect.flatMap((issue) => {
          if (issue === undefined) return Effect.succeed(decoded)
          return Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "bounded semantic conflict value",
                observed: issue._tag
              })
            })
          )
        })
      )
    })
  )

export const encode = <S extends Document.WireSchema,>(
  schema: S,
  value: S["Type"]
): Effect.Effect<Schema.Json, ReplicaError.ReplicaError> =>
  Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "schema coded JSON",
          observed: String(cause)
        })
      })
    )
  )

export const decode = <S extends Document.WireSchema,>(
  schema: S,
  value: Schema.Json
): Effect.Effect<S["Type"], ReplicaError.ReplicaError> =>
  Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "schema coded JSON",
          observed: String(cause)
        })
      })
    )
  )

export const encodeOutcome = <A extends Document.WireSchema, E extends Document.WireSchema,>(
  success: A,
  error: E,
  outcome: CommandOutcome.CommandOutcome<A["Type"], E["Type"]>
): Effect.Effect<
  CommandOutcome.CommandOutcome<Schema.Json, Schema.Json>,
  ReplicaError.ReplicaError
> => {
  switch (outcome._tag) {
    case "DurablyCommittedLocal":
      return encode(success, outcome.value).pipe(Effect.map((encodedValue) => ({ ...outcome, value: encodedValue })))
    case "Rejected":
      return encode(error, outcome.error).pipe(Effect.map((encodedError) => ({ ...outcome, error: encodedError })))
    case "OutcomeUnknown":
      return Effect.succeed(outcome)
    default:
      return Effect.die(outcome)
  }
}

export const decodeOutcome = <A extends Document.WireSchema, E extends Document.WireSchema,>(
  success: A,
  error: E,
  outcome: CommandOutcome.CommandOutcome<Schema.Json, Schema.Json>
): Effect.Effect<
  CommandOutcome.CommandOutcome<A["Type"], E["Type"]>,
  ReplicaError.ReplicaError
> => {
  switch (outcome._tag) {
    case "DurablyCommittedLocal":
      return decode(success, outcome.value).pipe(Effect.map((decodedValue) => ({ ...outcome, value: decodedValue })))
    case "Rejected":
      return decode(error, outcome.error).pipe(Effect.map((decodedError) => ({ ...outcome, error: decodedError })))
    case "OutcomeUnknown":
      return Effect.succeed(outcome)
    default:
      return Effect.die(outcome)
  }
}
