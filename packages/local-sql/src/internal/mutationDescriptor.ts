import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"

export const validate = (
  definition: Definition.Any,
  mutation: Mutation.Any
): Effect.Effect<void, ReplicaError.ProtocolInvalid> => {
  const registered = definition.mutationByName.get(mutation.name)
  if (registered === mutation) return Effect.void
  return Effect.fail(new ReplicaError.ProtocolInvalid({ message: `Unknown mutation: ${mutation.name}` }))
}
