import * as Schema from "effect/Schema"
import * as Protocol from "@lucas-barake/effect-local/Protocol"

export class TerminalRejection extends Schema.TaggedErrorClass<TerminalRejection>(
  "@lucas-barake/effect-local-sql/TerminalRejection"
)("TerminalRejection", { origin: Protocol.RejectionOrigin, rejection: Schema.Json }) {}
