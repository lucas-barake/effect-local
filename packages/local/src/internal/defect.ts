import * as Effect from "effect/Effect"

export const invalid = (message: string): never => Effect.runSync(Effect.die(new TypeError(message)))
