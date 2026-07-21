import type * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

export const encode = <S extends Document.WireSchema,>(schema: S, value: S["Type"]) =>
  Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: { _tag: "ProtocolMismatch", expected: "schema coded JSON", observed: String(cause) }
      })
    )
  ) as Effect.Effect<Schema.Json, ReplicaError.ReplicaError>

export const decode = <S extends Document.WireSchema,>(schema: S, value: Schema.Json) =>
  Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: { _tag: "ProtocolMismatch", expected: "schema coded JSON", observed: String(cause) }
      })
    )
  ) as Effect.Effect<S["Type"], ReplicaError.ReplicaError>

export const encodeOutcome = <A extends Document.WireSchema, E extends Document.WireSchema,>(
  success: A,
  error: E,
  outcome: CommandOutcome.CommandOutcome<A["Type"], E["Type"]>
): Effect.Effect<
  CommandOutcome.CommandOutcome<Schema.Json, Schema.Json>,
  ReplicaError.ReplicaError
> => {
  switch (outcome._tag) {
    case "DurablyCommittedLocal":
      return encode(success, outcome.value).pipe(Effect.map((value) => ({ ...outcome, value })))
    case "Rejected":
      return encode(error, outcome.error).pipe(Effect.map((error) => ({ ...outcome, error })))
    case "OutcomeUnknown":
      return Effect.succeed(outcome)
  }
}

export const decodeOutcome = <A extends Document.WireSchema, E extends Document.WireSchema,>(
  success: A,
  error: E,
  outcome: CommandOutcome.CommandOutcome<Schema.Json, Schema.Json>
): Effect.Effect<
  CommandOutcome.CommandOutcome<A["Type"], E["Type"]>,
  ReplicaError.ReplicaError
> => {
  switch (outcome._tag) {
    case "DurablyCommittedLocal":
      return decode(success, outcome.value).pipe(Effect.map((value) => ({ ...outcome, value })))
    case "Rejected":
      return decode(error, outcome.error).pipe(Effect.map((error) => ({ ...outcome, error })))
    case "OutcomeUnknown":
      return Effect.succeed(outcome)
  }
}
