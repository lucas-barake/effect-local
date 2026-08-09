/**
 * Effect `Crypto` service backed by `expo-crypto`.
 *
 * Random bytes come from expo-crypto's native CSPRNG and digests from its
 * native SHA implementation, so this layer satisfies every `Crypto.Crypto`
 * requirement in Effect Local without touching Node or Web APIs that Hermes
 * does not provide.
 *
 * @since 0.1.0
 */
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PlatformError from "effect/PlatformError"
import * as ExpoCrypto from "expo-crypto"

/**
 * `Context.Reference` for the expo-crypto module used by {@link layer}.
 * Defaults to the `expo-crypto` package itself; override in tests or when an
 * app wraps expo-crypto with its own entropy source.
 *
 * @category references
 * @since 0.1.0
 */
export const ExpoCryptoModule = Context.Reference<typeof ExpoCrypto>(
  "@lucas-barake/effect-local-react-native/Crypto/ExpoCryptoModule",
  { defaultValue: () => ExpoCrypto }
)

/**
 * Layer providing `Crypto.Crypto` backed by expo-crypto's native APIs.
 *
 * @category layers
 * @since 0.1.0
 */
export const layer: Layer.Layer<Crypto.Crypto> = Layer.effect(
  Crypto.Crypto,
  Effect.map(ExpoCryptoModule, (expoCrypto) => {
    const crypto = Crypto.make({
      randomBytes: (size) => expoCrypto.getRandomValues(new Uint8Array(size)),
      digest: (algorithm, data) =>
        Effect.map(
          Effect.tryPromise({
            try: () =>
              expoCrypto.digest(algorithm as ExpoCrypto.CryptoDigestAlgorithm, data as unknown as BufferSource),
            catch: (cause) =>
              PlatformError.systemError({
                module: "Crypto",
                method: "digest",
                _tag: "Unknown",
                description: "Could not compute digest",
                cause
              })
          }),
          (buffer) => new Uint8Array(buffer)
        )
    })
    return Crypto.Crypto.of({
      ...crypto,
      randomBytes: (size) =>
        crypto.randomBytes(size).pipe(
          Effect.catchDefect((cause) =>
            Effect.fail(PlatformError.systemError({
              module: "Crypto",
              method: "randomBytes",
              _tag: "Unknown",
              description: "Could not generate random bytes",
              cause
            }))
          )
        ),
      randomUUIDv4: crypto.randomUUIDv4.pipe(
        Effect.catchDefect((cause) =>
          Effect.fail(PlatformError.systemError({
            module: "Crypto",
            method: "randomUUIDv4",
            _tag: "Unknown",
            description: "Could not generate UUIDv4",
            cause
          }))
        )
      ),
      randomUUIDv7: crypto.randomUUIDv7.pipe(
        Effect.catchDefect((cause) =>
          Effect.fail(PlatformError.systemError({
            module: "Crypto",
            method: "randomUUIDv7",
            _tag: "Unknown",
            description: "Could not generate UUIDv7",
            cause
          }))
        )
      )
    })
  })
)
