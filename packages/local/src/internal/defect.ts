import * as Effect from "effect/Effect"

export const invalid = (message: string): never => {
  // Synchronous constructors call this invariant boundary and must throw before returning an invalid value.
  // oxlint-disable-next-line effect-local/noManualEffectBoundary
  return Effect.die(new TypeError(message)).pipe(Effect.runSync)
}
