import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Encoding from "effect/Encoding"
import * as Schema from "effect/Schema"
import * as ReplicaError from "./ReplicaError.js"

// Non-JSON values encode as sentinel-prefixed strings, and plain strings that start
// with the sentinel gain one more, so no input can forge another value's encoding.
const sentinel = "\u001d"

// instanceof is realm-bound, so cross-realm values would silently normalize as plain
// objects; these brand checks keep one logical value on one encoding across realms.
const isDate = (value: object): value is Date => Object.prototype.toString.call(value) === "[object Date]"

const isUint8Array = (value: object): value is Uint8Array =>
  ArrayBuffer.isView(value) && Object.prototype.toString.call(value) === "[object Uint8Array]"

const normalize = (value: unknown, ancestors: WeakSet<object>): unknown => {
  switch (typeof value) {
    case "string":
      return value.startsWith(sentinel) ? sentinel + value : value
    case "bigint":
      return `${sentinel}bigint:${value}`
    case "number":
      return Number.isFinite(value) ? value : `${sentinel}number:${value}`
    case "undefined":
      return `${sentinel}undefined`
    case "function":
    case "symbol":
      return `${sentinel}${typeof value}:${String(value)}`
  }
  if (value === null || typeof value !== "object") return value
  if (isDate(value)) return `${sentinel}date:${value.toISOString()}`
  if (isUint8Array(value)) return `${sentinel}bytes:${Encoding.encodeHex(value)}`
  if (ArrayBuffer.isView(value)) {
    return `${sentinel}view:${Object.prototype.toString.call(value).slice(8, -1)}:${
      Encoding.encodeHex(new Uint8Array(value.buffer, value.byteOffset, value.byteLength))
    }`
  }
  if (ancestors.has(value)) return `${sentinel}circular`
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
  Effect.try({
    try: () => stringify(value),
    catch: (cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.CanonicalEncodeError({ cause })
      })
  }).pipe(
    Effect.flatMap((input) =>
      Crypto.Crypto.use((crypto) => crypto.digest("SHA-256", new TextEncoder().encode(input))).pipe(
        Effect.map(Encoding.encodeHex),
        Effect.mapError((cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause })
          })
        )
      )
    )
  )
