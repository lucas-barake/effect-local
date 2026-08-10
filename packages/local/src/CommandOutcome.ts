import * as Effect from "effect/Effect"
import * as Match from "effect/Match"
import * as Schema from "effect/Schema"
import type * as Document from "./Document.js"
import * as Identity from "./Identity.js"
import * as ReplicaError from "./ReplicaError.js"

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

/**
 * Projects an outcome into the Effect channels: the committed value on success, the declared error
 * `E` unwrapped on rejection, and `ReplicaError.CommandOutcomeUnknown` when durability could not be
 * established.
 *
 * `create`, `mutate` and `delete` already do this for you. Reach for it on a `lookup*` result, where
 * an outcome is the honest answer because "what happened to this id" genuinely has three of them.
 *
 * `cause` is only known where the ambiguity arose, which for a lookup is the absence of a receipt.
 */
/**
 * The inverse of `committedOrFail`, for a boundary that has to carry an outcome as a value.
 *
 * A `ReplicaError` stays in the error channel, because it says the replica failed rather than that
 * the command produced a result. The exception is `CommandOutcomeUnknown`, which is precisely the
 * third command result: it becomes `OutcomeUnknown` so the peer can resolve it with its own lookup.
 * Its `cause` does not survive, because it describes this side's ambiguity and not the peer's.
 */
export function toOutcome<A, E, R,>(
  commandId: Identity.CommandId,
  effect: Effect.Effect<A, E, R>
): Effect.Effect<
  CommandOutcome<A, Exclude<E, ReplicaError.ReplicaError>>,
  ReplicaError.ReplicaError,
  R
>
export function toOutcome<A, E, R,>(
  commandId: Identity.CommandId,
  effect: Effect.Effect<A, E, R>
): Effect.Effect<CommandOutcome<A, E>, ReplicaError.ReplicaError, R> {
  return effect.pipe(
    Effect.map((value): CommandOutcome<A, E> => durablyCommitted(commandId, value)),
    Effect.catch((error): Effect.Effect<CommandOutcome<A, E>, ReplicaError.ReplicaError> => {
      if (ReplicaError.isReplicaError(error)) {
        if (error.reason._tag === "CommandOutcomeUnknown") {
          return Effect.succeed(unknown(error.reason.commandId))
        }
        return Effect.fail(error)
      }
      return Effect.succeed(rejected(commandId, error))
    })
  )
}

export const committedOrFail = <A, E,>(
  self: CommandOutcome<A, E>,
  cause?: unknown
): Effect.Effect<A, E | ReplicaError.ReplicaError> => {
  if (self._tag === "DurablyCommittedLocal") return Effect.succeed(self.value)
  if (self._tag === "Rejected") return Effect.fail(self.error)
  return Effect.fail(
    new ReplicaError.ReplicaError({
      reason: new ReplicaError.CommandOutcomeUnknown({ commandId: self.commandId, cause })
    })
  )
}
