import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Encoding from "effect/Encoding"
import * as Schema from "effect/Schema"
import * as ReplicaError from "./ReplicaError.js"

const normalize = (value: unknown, ancestors: WeakSet<object>): unknown => {
  if (typeof value === "function" || typeof value === "symbol") return String(value)
  if (typeof value === "bigint") return value.toString()
  if (value === null || typeof value !== "object") return value
  if (value instanceof Date) return { _tag: "Date", value: value.toISOString() }
  if (value instanceof Uint8Array) {
    return { _tag: "Uint8Array", value: Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join("") }
  }
  if (ancestors.has(value)) return "[Circular]"
  ancestors.add(value)
  const result = Array.isArray(value)
    ? value.map((item) => normalize(item, ancestors))
    : Object.fromEntries(
      Object.keys(value).toSorted().map((key) => [key, normalize(Reflect.get(value, key), ancestors)])
    )
  ancestors.delete(value)
  return result
}

export const stringify = (value: unknown): string =>
  Schema.encodeSync(Schema.UnknownFromJsonString)(normalize(value, new WeakSet()))

export const hash = (value: unknown): string => {
  const input = stringify(value)
  let current = 0xcbf29ce484222325n
  for (let index = 0; index < input.length; index++) {
    current ^= BigInt(input.charCodeAt(index))
    current = BigInt.asUintN(64, current * 0x100000001b3n)
  }
  return current.toString(16).padStart(16, "0")
}

export const digest = (value: unknown) =>
  Crypto.Crypto.use((crypto) => crypto.digest("SHA-256", new TextEncoder().encode(stringify(value)))).pipe(
    Effect.map(Encoding.encodeHex),
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.StorageUnavailable({ cause })
      })
    )
  )
