import * as Cause from "effect/Cause"
import * as Schema from "effect/Schema"

const isRemoteDefect = Schema.is(Schema.Struct({ _tag: Schema.Literal("RemoteDefect") }))

export const hasRemoteDefect = <E,>(cause: Cause.Cause<E>): boolean =>
  cause.reasons.some((reason) => Cause.isDieReason(reason) && isRemoteDefect(reason.defect))
