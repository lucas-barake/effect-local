import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Schema from "effect/Schema"

export class TerminalRejection extends Schema.TaggedError<TerminalRejection>(
  "@lucas-barake/effect-local-sql/TerminalRejection"
)("TerminalRejection", { origin: Protocol.RejectionOrigin, rejection: Schema.Json }) {}
