import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Codec from "./codec.js"
import type * as Rows from "./rows.js"

export const decode = (row: typeof Rows.ServerLogRow.Type) =>
  Effect.gen(function*() {
    const entry = yield* Codec.parse(row.entry_json).pipe(
      Effect.flatMap((value) => Codec.decode(Protocol.AcceptedMutation, value))
    )
    if (
      entry.spaceId !== row.space_id ||
      entry.sequence !== row.server_sequence ||
      entry.clientId !== row.client_id ||
      entry.membershipIncarnation !== row.membership_incarnation ||
      entry.localSequence !== row.local_sequence ||
      entry.mutationId !== row.mutation_id ||
      entry.digest !== row.digest ||
      entry.sourceSchema.version !== row.source_schema_version ||
      entry.sourceSchema.hash !== row.source_schema_hash ||
      row.entry_bytes !== (yield* Protocol.encodedBytesEffect(entry)) ||
      row.entry_json !== (yield* Codec.stringify(entry))
    ) {
      return yield* new ReplicaError.StorageCorrupt({
        message: `Accepted entry ${row.server_sequence} conflicts with its durable metadata`
      })
    }
    return entry
  })
