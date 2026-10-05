import * as Cause from "effect/Cause"
import * as Schema from "effect/Schema"

export const findDecodeDefect = <E,>(cause: Cause.Cause<E>): Schema.SchemaError | undefined => {
  for (const reason of cause.reasons) {
    if (Cause.isDieReason(reason) && Schema.isSchemaError(reason.defect)) return reason.defect
  }
  return undefined
}
