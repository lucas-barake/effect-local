import * as Arr from "effect/Array"
import * as Chunk from "effect/Chunk"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Hex from "effect/encoding/Hex"
import * as HashMap from "effect/HashMap"
import * as HashSet from "effect/HashSet"
import * as Order from "effect/Order"
import * as Schema from "effect/Schema"
import * as ReplicaError from "./ReplicaError.js"

// Non-JSON values encode as sentinel-prefixed strings, and plain strings that start
// with the sentinel gain one more, so no input can forge another value's encoding.
const sentinel = "\u001d"

// instanceof is realm-bound, so cross-realm values would silently normalize as plain
// objects; these brand checks keep one logical value on one encoding across realms.
const isDate = (value: object): value is Date => Object.prototype.toString.call(value) === "[object Date]"

const isMap = (value: object): value is Map<unknown, unknown> =>
  Object.prototype.toString.call(value) === "[object Map]"

const isSet = (value: object): value is Set<unknown> => Object.prototype.toString.call(value) === "[object Set]"

const isIterable = (value: object): boolean => Symbol.iterator in value

const isUint8Array = (value: object): value is Uint8Array =>
  ArrayBuffer.isView(value) && Object.prototype.toString.call(value) === "[object Uint8Array]"

// Canonical hashing, cache keys, and SQL interpolation require this public codec to return synchronously.
// oxlint-disable-next-line effect-local/noManualEffectBoundary
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))

const sortedMembers = (tag: string, members: Array<unknown>): Array<unknown> => [
  tag,
  ...Arr.sortWith(members, encodeJson, Order.String)
]

const normalize = (value: unknown, ancestors: WeakSet<object>): unknown => {
  switch (typeof value) {
    case "string":
      if (value.startsWith(sentinel)) return sentinel + value
      return value
    case "bigint":
      return `${sentinel}bigint:${value}`
    case "number":
      if (Number.isFinite(value)) return value
      return `${sentinel}number:${value}`
    case "undefined":
      return `${sentinel}undefined`
    case "function":
    case "symbol":
      return `${sentinel}${typeof value}:${String(value)}`
  }
  if (value === null || typeof value !== "object") return value
  if (isDate(value)) return `${sentinel}date:${value.toISOString()}`
  if (isUint8Array(value)) return `${sentinel}bytes:${Hex.encode(value)}`
  if (ArrayBuffer.isView(value)) {
    return `${sentinel}view:${Object.prototype.toString.call(value).slice(8, -1)}:${
      Hex.encode(new Uint8Array(value.buffer, value.byteOffset, value.byteLength))
    }`
  }
  if (ancestors.has(value)) return `${sentinel}circular`
  ancestors.add(value)
  let result: unknown
  if (Array.isArray(value)) {
    result = value.map((item) => normalize(item, ancestors))
  } else if (isMap(value)) {
    const entries = Map.prototype.entries.call(value)
    const members = Array.from(entries, ([key, item]) => [normalize(key, ancestors), normalize(item, ancestors)])
    result = sortedMembers(`${sentinel}map`, members)
  } else if (isSet(value)) {
    const items = Set.prototype.values.call(value)
    result = sortedMembers(`${sentinel}set`, Array.from(items, (item) => normalize(item, ancestors)))
  } else if (HashSet.isHashSet(value) && isIterable(value)) {
    result = sortedMembers(`${sentinel}hashset`, Array.from(value, (item) => normalize(item, ancestors)))
  } else if (HashMap.isHashMap(value) && isIterable(value)) {
    const entries = HashMap.entries(value)
    const members = Array.from(entries, ([key, item]) => [normalize(key, ancestors), normalize(item, ancestors)])
    result = sortedMembers(`${sentinel}hashmap`, members)
  } else if (Chunk.isChunk(value) && isIterable(value)) {
    const items = Chunk.toReadonlyArray(value).map((item) => normalize(item, ancestors))
    result = [`${sentinel}chunk`, ...items]
  } else {
    const entries = Object.keys(value).sort().map((key) => {
      return [key, normalize(Reflect.get(value, key), ancestors)] as const
    })
    result = Object.fromEntries(entries)
  }
  ancestors.delete(value)
  return result
}

export const stringify = (value: unknown): string => encodeJson(normalize(value, new WeakSet()))

export const stringifyEffect = (value: unknown): Effect.Effect<string, ReplicaError.CanonicalEncodeError> =>
  Effect.try({
    try: () => stringify(value),
    catch: (cause) => new ReplicaError.CanonicalEncodeError({ cause })
  })

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
  stringifyEffect(value).pipe(
    Effect.flatMap((input) =>
      Crypto.Crypto.use((crypto) => crypto.digest("SHA-256", new TextEncoder().encode(input))).pipe(
        Effect.map(Hex.encode),
        Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause }))
      )
    )
  )
