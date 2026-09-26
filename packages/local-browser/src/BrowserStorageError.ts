import * as Schema from "effect/Schema"

export class BrowserStorageError extends Schema.TaggedError<BrowserStorageError>(
  "@lucas-barake/effect-local-browser/BrowserStorageError"
)("BrowserStorageError", {
  operation: Schema.Literals(["read", "write", "decode"]),
  key: Schema.String,
  cause: Schema.Defect()
}) {}
