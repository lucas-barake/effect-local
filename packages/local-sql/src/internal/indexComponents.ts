import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as SecondaryIndex from "@lucas-barake/effect-local/SecondaryIndex"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

export type SqlValue = string | number

export const affinitySql = { text: "TEXT", real: "REAL", integer: "INTEGER" } as const

export const encodeComponent = (
  component: SecondaryIndex.ComponentInput,
  value: unknown
): Effect.Effect<SqlValue, ReplicaError.StorageCorrupt> =>
  Schema.encodeUnknownEffect(component.schema)(value).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.StorageCorrupt({
        message: `Index component ${component.name} failed Schema encoding`,
        cause
      })
    ),
    Effect.flatMap((encoded): Effect.Effect<SqlValue, ReplicaError.StorageCorrupt> => {
      if (component.affinity === "text" && typeof encoded === "string") return Effect.succeed<SqlValue>(encoded)
      if (component.affinity === "real" && typeof encoded === "number" && Number.isFinite(encoded)) {
        return Effect.succeed<SqlValue>(encoded)
      }
      if (component.affinity === "integer") {
        if (typeof encoded === "boolean") {
          if (encoded) return Effect.succeed<SqlValue>(1)
          return Effect.succeed<SqlValue>(0)
        }
        if (typeof encoded === "number" && Number.isSafeInteger(encoded)) return Effect.succeed<SqlValue>(encoded)
      }
      return Effect.fail(
        new ReplicaError.StorageCorrupt({
          message: `Index component ${component.name} encoded outside its SQLite affinity`
        })
      )
    })
  )

export const encodedComponents = (
  index: SecondaryIndex.Any,
  value: unknown
): Effect.Effect<ReadonlyArray<SqlValue>, ReplicaError.StorageCorrupt> =>
  Effect.forEach(
    [...index.partition, ...index.sort],
    (component) => encodeComponent(component, component.extract(value))
  )

export const encodedPrimitive = (
  component: SecondaryIndex.ComponentInput,
  encoded: string | number | boolean
): Effect.Effect<SqlValue, ReplicaError.StorageCorrupt> => {
  if (component.affinity === "text" && typeof encoded === "string") return Effect.succeed<SqlValue>(encoded)
  if (component.affinity === "real" && typeof encoded === "number" && Number.isFinite(encoded)) {
    return Effect.succeed<SqlValue>(encoded)
  }
  if (component.affinity === "integer") {
    if (typeof encoded === "boolean") {
      if (encoded) return Effect.succeed<SqlValue>(1)
      return Effect.succeed<SqlValue>(0)
    }
    if (typeof encoded === "number" && Number.isSafeInteger(encoded)) return Effect.succeed<SqlValue>(encoded)
  }
  return Effect.fail(
    new ReplicaError.StorageCorrupt({
      message: `Index component ${component.name} value does not match its SQLite affinity`
    })
  )
}
