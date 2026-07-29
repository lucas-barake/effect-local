import type * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

export interface ConflictLimits {
  readonly maxConflictDepth: number
  readonly maxConflictNodes: number
  readonly maxConflictAlternatives: number
  readonly maxConflictPathSegments: number
  readonly maxConflictValueBytes: number
}

type PreflightResult =
  | { readonly _tag: "Success" }
  | { readonly _tag: "Failure"; readonly observed: string }

type PendingValue = {
  readonly _tag: "Value"
  readonly value: unknown
  readonly depth: number
}

type PendingExit = {
  readonly _tag: "Exit"
  readonly value: object
}

const unsafeKeys = new Set([
  ...Object.getOwnPropertyNames(Object.prototype),
  "prototype"
])

const utf8Bytes = (value: string): number => new TextEncoder().encode(value).byteLength

const preflightConflictJson = (
  input: unknown,
  limits: ConflictLimits
): PreflightResult => {
  const active = new WeakSet<object>()
  const pending: Array<PendingValue | PendingExit> = [{ _tag: "Value", value: input, depth: 0 }]
  let nodes = 0
  let bytes = 0

  const addBytes = (amount: number): PreflightResult | undefined => {
    bytes += amount
    return bytes > limits.maxConflictValueBytes
      ? { _tag: "Failure", observed: "conflict JSON exceeds the advertised byte limit" }
      : undefined
  }

  try {
    while (pending.length > 0) {
      const next = pending.pop()!
      if (next._tag === "Exit") {
        active.delete(next.value)
        continue
      }

      nodes += 1
      if (nodes > limits.maxConflictNodes) {
        return { _tag: "Failure", observed: "conflict JSON exceeds the advertised node limit" }
      }
      if (next.depth > limits.maxConflictDepth) {
        return { _tag: "Failure", observed: "conflict JSON exceeds the advertised depth limit" }
      }

      const value = next.value
      if (value === null) {
        const failure = addBytes(4)
        if (failure !== undefined) return failure
        continue
      }
      switch (typeof value) {
        case "boolean": {
          const failure = addBytes(value ? 4 : 5)
          if (failure !== undefined) return failure
          continue
        }
        case "number": {
          if (!Number.isFinite(value)) {
            return { _tag: "Failure", observed: "conflict JSON contains a nonfinite number" }
          }
          const failure = addBytes(String(value).length)
          if (failure !== undefined) return failure
          continue
        }
        case "string": {
          const failure = addBytes(utf8Bytes(JSON.stringify(value)))
          if (failure !== undefined) return failure
          continue
        }
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
        const keys = Reflect.ownKeys(value)
        if (
          keys.some((key) =>
            typeof key !== "string" ||
            (key !== "length" && (!/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= value.length))
          )
        ) {
          return { _tag: "Failure", observed: "conflict JSON contains unsupported array properties" }
        }
        for (let index = 0; index < value.length; index++) {
          const descriptor = Object.getOwnPropertyDescriptor(value, String(index))
          if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
            return { _tag: "Failure", observed: "conflict JSON contains a sparse or accessor array entry" }
          }
        }
        const punctuation = value.length === 0 ? 2 : value.length + 1
        const failure = addBytes(punctuation)
        if (failure !== undefined) return failure
        for (let index = value.length - 1; index >= 0; index--) {
          pending.push({ _tag: "Value", value: value[index], depth: next.depth + 1 })
        }
        continue
      }

      const prototype = Object.getPrototypeOf(value)
      if (prototype !== Object.prototype && prototype !== null) {
        return { _tag: "Failure", observed: "conflict JSON contains an unsupported object prototype" }
      }
      const keys = Reflect.ownKeys(value)
      if (keys.some((key) => typeof key !== "string")) {
        return { _tag: "Failure", observed: "conflict JSON contains a symbol key" }
      }
      const stringKeys = keys as Array<string>
      const data = Array.from<unknown>({ length: stringKeys.length })
      for (let index = 0; index < stringKeys.length; index++) {
        const key = stringKeys[index]!
        if (unsafeKeys.has(key)) {
          return { _tag: "Failure", observed: "conflict JSON contains a prototype sensitive key" }
        }
        const descriptor = Object.getOwnPropertyDescriptor(value, key)
        if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
          return { _tag: "Failure", observed: "conflict JSON contains an accessor or hidden property" }
        }
        data[index] = descriptor.value
        const failure = addBytes(utf8Bytes(JSON.stringify(key)) + 1)
        if (failure !== undefined) return failure
      }
      const alternatives = stringKeys.indexOf("alternatives")
      if (
        alternatives !== -1 &&
        Array.isArray(data[alternatives]) &&
        data[alternatives].length > limits.maxConflictAlternatives
      ) {
        return { _tag: "Failure", observed: "conflict JSON exceeds the advertised alternative limit" }
      }
      const parents = stringKeys.indexOf("parents")
      const target = stringKeys.indexOf("target")
      if (
        parents !== -1 &&
        target !== -1 &&
        Array.isArray(data[parents]) &&
        data[parents].length + 1 > limits.maxConflictPathSegments
      ) {
        return { _tag: "Failure", observed: "conflict JSON exceeds the advertised path limit" }
      }
      const punctuation = stringKeys.length === 0 ? 2 : stringKeys.length + 1
      const failure = addBytes(punctuation)
      if (failure !== undefined) return failure
      for (let index = data.length - 1; index >= 0; index--) {
        pending.push({ _tag: "Value", value: data[index], depth: next.depth + 1 })
      }
    }
  } catch {
    return { _tag: "Failure", observed: "conflict JSON could not be inspected safely" }
  }

  return { _tag: "Success" }
}

export const encodeConflictText = (
  value: unknown,
  limits: ConflictLimits
): Effect.Effect<string, ReplicaError.ReplicaError> => {
  const preflight = preflightConflictJson(value, limits)
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
  return Effect.try({
    try: () => JSON.stringify(value),
    catch: (cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "bounded conflict JSON text",
          observed: String(cause)
        })
      })
  })
}

export const decodeConflictText = (
  value: string,
  limits: ConflictLimits
): Effect.Effect<unknown, ReplicaError.ReplicaError> => {
  if (utf8Bytes(value) > limits.maxConflictValueBytes) {
    return Effect.fail(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "bounded conflict JSON text",
          observed: "conflict JSON exceeds the advertised byte limit"
        })
      })
    )
  }
  return Effect.try({
    try: () => JSON.parse(value) as unknown,
    catch: (cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "bounded conflict JSON text",
          observed: String(cause)
        })
      })
  }).pipe(
    Effect.flatMap((decoded) => {
      const preflight = preflightConflictJson(decoded, limits)
      return preflight._tag === "Success"
        ? Effect.succeed(decoded)
        : Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "bounded conflict JSON text",
              observed: preflight.observed
            })
          })
        )
    })
  )
}

export const encodeConflict = <S extends Document.WireSchema,>(
  schema: S,
  value: S["Type"],
  limits: ConflictLimits
): Effect.Effect<string, ReplicaError.ReplicaError> =>
  Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "schema coded conflict JSON",
          observed: String(cause)
        })
      })
    ),
    Effect.flatMap((encoded) => encodeConflictText(encoded, limits))
  )

export const decodeConflict = <S extends Document.WireSchema,>(
  schema: S,
  value: string,
  limits: ConflictLimits
): Effect.Effect<S["Type"], ReplicaError.ReplicaError> =>
  decodeConflictText(value, limits).pipe(
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
    )
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
      return encode(success, outcome.value).pipe(Effect.map((value) => ({ ...outcome, value })))
    case "Rejected":
      return encode(error, outcome.error).pipe(Effect.map((error) => ({ ...outcome, error })))
    case "OutcomeUnknown":
      return Effect.succeed(outcome)
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
      return decode(success, outcome.value).pipe(Effect.map((value) => ({ ...outcome, value })))
    case "Rejected":
      return decode(error, outcome.error).pipe(Effect.map((error) => ({ ...outcome, error })))
    case "OutcomeUnknown":
      return Effect.succeed(outcome)
  }
}
