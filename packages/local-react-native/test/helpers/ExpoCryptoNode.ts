/**
 * Node-backed fake of the `expo-crypto` JS API surface, used only in tests via
 * the vitest module alias. expo-crypto is a native module, so this re-exports
 * the same contract over `node:crypto`.
 */
import { createHash, getRandomValues as nodeGetRandomValues, randomUUID as nodeRandomUUID } from "node:crypto"

export enum CryptoDigestAlgorithm {
  SHA1 = "SHA-1",
  SHA256 = "SHA-256",
  SHA384 = "SHA-384",
  SHA512 = "SHA-512",
  MD5 = "MD5"
}

export const getRandomBytes = (byteCount: number): Uint8Array => nodeGetRandomValues(new Uint8Array(byteCount))

export function getRandomValues<T extends Uint8Array,>(typedArray: T): T {
  return nodeGetRandomValues(typedArray as Uint8Array<ArrayBuffer>) as T
}

export const randomUUID = (): string => nodeRandomUUID()

export const digest = (algorithm: CryptoDigestAlgorithm, data: ArrayBuffer | ArrayBufferView): Promise<ArrayBuffer> => {
  const bytes = ArrayBuffer.isView(data)
    ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
    : new Uint8Array(data)
  const hash = createHash(algorithm.toLowerCase().replace("-", ""))
  hash.update(bytes)
  const output = hash.digest()
  return Promise.resolve(output.buffer.slice(output.byteOffset, output.byteOffset + output.byteLength) as ArrayBuffer)
}
