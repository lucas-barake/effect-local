import { createHash, randomFillSync } from "node:crypto"

export const cryptoProbe = {
  failNextDigest: false,
  filled: new Uint8Array(0),
  reset() {
    cryptoProbe.failNextDigest = false
    cryptoProbe.filled = new Uint8Array(0)
  }
}

const algorithms: Record<string, string> = {
  "SHA-1": "sha1",
  "SHA-256": "sha256",
  "SHA-384": "sha384",
  "SHA-512": "sha512"
}

const bytesOf = (data: ArrayBufferView | ArrayBuffer) => {
  if (data instanceof ArrayBuffer) return new Uint8Array(data)
  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
}

export const ExpoCrypto = {
  getRandomValues(array: Uint8Array<ArrayBuffer>) {
    randomFillSync(array)
    cryptoProbe.filled = Uint8Array.from(array)
    return array
  },
  digest(algorithm: string, output: ArrayBufferView, data: ArrayBufferView | ArrayBuffer) {
    if (cryptoProbe.failNextDigest) {
      cryptoProbe.failNextDigest = false
      throw new Error("native digest unavailable")
    }
    bytesOf(output).set(createHash(algorithms[algorithm]).update(bytesOf(data)).digest())
  }
}

export const ExpoCryptoAES = {
  EncryptionKey: class EncryptionKey {},
  SealedData: class SealedData {}
}
