import type * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"

const blockSize = 64
const innerPad = 0x36
const outerPad = 0x5c

export const sha256 = Effect.fnUntraced(function*(crypto: Crypto.Crypto, key: Uint8Array, message: Uint8Array) {
  let normalized = key
  if (normalized.length > blockSize) normalized = yield* crypto.digest("SHA-256", normalized)
  const inner = new Uint8Array(blockSize + message.length)
  const outer = new Uint8Array(blockSize + 32)
  for (let index = 0; index < blockSize; index++) {
    const byte = normalized[index] ?? 0
    inner[index] = byte ^ innerPad
    outer[index] = byte ^ outerPad
  }
  inner.set(message, blockSize)
  outer.set(yield* crypto.digest("SHA-256", inner), blockSize)
  return yield* crypto.digest("SHA-256", outer)
})

export const constantTimeEqual = (left: Uint8Array, right: Uint8Array): boolean => {
  if (left.length !== right.length) return false
  let difference = 0
  for (let index = 0; index < left.length; index++) difference |= left[index] ^ right[index]
  return difference === 0
}
