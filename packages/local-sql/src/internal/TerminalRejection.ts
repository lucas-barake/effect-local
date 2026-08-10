import * as Schema from "effect/Schema"

export class TerminalRejection extends Schema.TaggedErrorClass<TerminalRejection>(
  "@lucas-barake/effect-local-sql/TerminalRejection"
)("TerminalRejection", { rejection: Schema.Json }) {}
