import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import type * as RpcSerialization from "effect/rpc/RpcSerialization"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"

const responseIssues = new WeakSet<object>()

const marking = <T, RD, RE,>(codec: Schema.Codec<T, unknown, RD, RE>): Schema.Codec<T, unknown, RD, RE> => {
  const decode = Schema.decodeUnknownEffect(codec)
  const encode = Schema.encodeEffect(codec)
  return Schema.Unknown.pipe(Schema.decodeTo(Schema.toType(codec), {
    decode: SchemaGetter.transformEffect((encoded: unknown) =>
      decode(encoded).pipe(Effect.mapError((error) => {
        responseIssues.add(error.issue)
        return error.issue
      }))
    ),
    encode: SchemaGetter.transformEffect((value: T) => encode(value).pipe(Effect.mapError((error) => error.issue)))
  }))
}

export const markingCodecFor = (codecFor: RpcSerialization.CodecFor): RpcSerialization.CodecFor => (schema) =>
  marking(codecFor(schema))

export const findDecodeDefect = <E,>(cause: Cause.Cause<E>): Schema.SchemaError | undefined => {
  for (const reason of cause.reasons) {
    if (!Cause.isDieReason(reason) || !Schema.isSchemaError(reason.defect)) continue
    const issue = reason.defect.issue
    if (issue._tag === "Encoding" && responseIssues.has(issue.issue)) return reason.defect
  }
  return undefined
}
