import { createHash, randomFillSync } from "node:crypto"

export const cryptoProbe = {
  randomValueCalls: 0,
  failNextDigest: false,
  failNextRandomValues: false,
  reset() {
    cryptoProbe.randomValueCalls = 0
    cryptoProbe.failNextDigest = false
    cryptoProbe.failNextRandomValues = false
  }
}

const algorithms: Record<string, string> = {
  "SHA-1": "sha1",
  "SHA-256": "sha256",
  "SHA-384": "sha384",
  "SHA-512": "sha512"
}

export const ExpoCrypto = {
  getRandomValues(array: Uint8Array<ArrayBuffer>) {
    cryptoProbe.randomValueCalls++
    if (cryptoProbe.failNextRandomValues) {
      cryptoProbe.failNextRandomValues = false
      throw new Error("native random source unavailable")
    }
    return randomFillSync(array)
  },
  digestAsync(algorithm: string, data: ArrayBufferView | ArrayBuffer): Promise<ArrayBuffer> {
    if (cryptoProbe.failNextDigest) {
      cryptoProbe.failNextDigest = false
      return Promise.reject(new Error("native digest unavailable"))
    }
    let bytes: Uint8Array
    if (data instanceof ArrayBuffer) bytes = new Uint8Array(data)
    else bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
    const digest = createHash(algorithms[algorithm]).update(bytes).digest()
    return Promise.resolve(digest.buffer.slice(digest.byteOffset, digest.byteOffset + digest.byteLength))
  }
}

export const ExpoCryptoAES = {
  EncryptionKey: class EncryptionKey {},
  SealedData: class SealedData {}
}
