import * as Schema from "effect/Schema"
import * as SchemaTransformation from "effect/SchemaTransformation"

const JsonString = Schema.String.pipe(
  Schema.decodeTo(Schema.Unknown, SchemaTransformation.fromJsonString())
)

export const encodeJson = (value: unknown): string => Schema.encodeSync(JsonString)(value)

export const decodeJson = (value: string): any => Schema.decodeSync(JsonString)(value)
