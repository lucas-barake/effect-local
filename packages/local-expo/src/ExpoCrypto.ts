import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PlatformError from "effect/PlatformError"
import * as ExpoCrypto from "expo-crypto"

const algorithms: Record<Crypto.DigestAlgorithm, ExpoCrypto.CryptoDigestAlgorithm> = {
  "SHA-1": ExpoCrypto.CryptoDigestAlgorithm.SHA1,
  "SHA-256": ExpoCrypto.CryptoDigestAlgorithm.SHA256,
  "SHA-384": ExpoCrypto.CryptoDigestAlgorithm.SHA384,
  "SHA-512": ExpoCrypto.CryptoDigestAlgorithm.SHA512
}

const randomBytes = (size: number): Uint8Array => ExpoCrypto.getRandomValues(new Uint8Array(size))

const digest: Crypto.Crypto["digest"] = (algorithm, data) =>
  Effect.tryPromise({
    try: () => ExpoCrypto.digest(algorithms[algorithm], new Uint8Array(data)),
    catch: (cause) =>
      PlatformError.systemError({
        module: "Crypto",
        method: "digest",
        _tag: "Unknown",
        description: "Could not compute digest",
        cause
      })
  }).pipe(Effect.map((buffer) => new Uint8Array(buffer)))

export const layer: Layer.Layer<Crypto.Crypto> = Layer.sync(Crypto.Crypto, () => Crypto.make({ randomBytes, digest }))
