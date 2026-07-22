import type * as Cause from "effect/Cause"
import type * as EffectSchema from "effect/Schema"

export type Schema = EffectSchema.Codec<
  Cause.YieldableError & { readonly _tag: string },
  unknown,
  never,
  never
>
