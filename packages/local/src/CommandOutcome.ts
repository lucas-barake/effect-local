import * as Effect from "effect/Effect"
import * as Match from "effect/Match"
import * as Schema from "effect/Schema"
import type * as Document from "./Document.js"
import * as Identity from "./Identity.js"

export interface Rejected<E,> {
  readonly _tag: "Rejected"
  readonly commandId: Identity.CommandId
  readonly error: E
}

export interface DurablyCommittedLocal<A,> {
  readonly _tag: "DurablyCommittedLocal"
  readonly commandId: Identity.CommandId
  readonly value: A
}

export interface OutcomeUnknown {
  readonly _tag: "OutcomeUnknown"
  readonly commandId: Identity.CommandId
}

export type CommandOutcome<A, E = never,> = Rejected<E> | DurablyCommittedLocal<A> | OutcomeUnknown

export const schema = <A extends Document.WireSchema, E extends Document.WireSchema,>(success: A, error: E) =>
  Schema.TaggedUnion({
    Rejected: { commandId: Identity.CommandId, error },
    DurablyCommittedLocal: { commandId: Identity.CommandId, value: success },
    OutcomeUnknown: { commandId: Identity.CommandId }
  })

export const rejected = <E,>(commandId: Identity.CommandId, error: E): Rejected<E> => ({
  _tag: "Rejected",
  commandId,
  error
})

export const durablyCommitted = <A,>(commandId: Identity.CommandId, value: A): DurablyCommittedLocal<A> => ({
  _tag: "DurablyCommittedLocal",
  commandId,
  value
})

export const unknown = (commandId: Identity.CommandId): OutcomeUnknown => ({ _tag: "OutcomeUnknown", commandId })

export const match = <A, E, B,>(
  self: CommandOutcome<A, E>,
  handlers: {
    readonly onRejected: (outcome: Rejected<E>) => B
    readonly onCommitted: (outcome: DurablyCommittedLocal<A>) => B
    readonly onUnknown: (outcome: OutcomeUnknown) => B
  }
): B =>
  Match.typeTags<CommandOutcome<A, E>, B>()({
    Rejected: handlers.onRejected,
    DurablyCommittedLocal: handlers.onCommitted,
    OutcomeUnknown: handlers.onUnknown
  })(self)

export const committedOrFail = <A, E,>(
  self: CommandOutcome<A, E>
): Effect.Effect<A, Rejected<E> | OutcomeUnknown> =>
  self._tag === "DurablyCommittedLocal" ? Effect.succeed(self.value) : Effect.fail(self)
