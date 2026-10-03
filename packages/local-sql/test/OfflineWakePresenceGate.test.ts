import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as SqlClient from "effect/sql/SqlClient"
import * as Stream from "effect/Stream"
import * as MutationRuntime from "../src/MutationRuntime.js"
import type * as OfflineWake from "../src/OfflineWake.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"
import { serverDatabases } from "./fixtures/ServerDatabase.js"
import { gateStatements } from "./fixtures/SqlGate.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000a01")
const claimedId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000a01")
const otherId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000a02")

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const offlineWake: OfflineWake.Options = {
  recipients: () => Effect.succeed([]),
  deliver: () => Effect.succeed("Delivered"),
  coalescingWindow: "1 second",
  pollInterval: "1 hour",
  retryDelay: "1 second",
  maximumRetryDelay: "1 minute",
  claimLeaseDuration: "30 seconds",
  hookTimeout: "10 seconds",
  presenceLeaseDuration: "30 seconds",
  presenceHeartbeatInterval: "20 seconds",
  claimBatchSize: 8,
  maximumConcurrentRecipientResolutions: 1,
  maximumConcurrentDeliveries: 1,
  maximumRecipientsPerSpace: 8
}

const provideReactivity = Effect.provide(Reactivity.layer)

const watchRequest = (clientId: Identity.ClientId) =>
  Protocol.WatchRequest.make({
    spaceId,
    clientId,
    schema: Domain.definition.schemaIdentity,
    scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
    cursor: null
  })

describe.each(serverDatabases)("offline wake presence registration ($dialect)", (database) => {
  it.effect(
    "a watch for one client opens while another client's registration waits on a delivery claim",
    Effect.fnUntraced(
      function*() {
        const pool = yield* database.client
        const gate = yield* gateStatements(pool, (statement) => {
          if (
            statement.includes("INSERT INTO effect_local_server_watch_presence") &&
            statement.includes("WHERE NOT EXISTS")
          ) return ["after"]
          return []
        })
        const store = yield* ServerStore.layerTrusted({ definition: Domain.definition, offlineWake }).pipe(
          Layer.provide(layerRuntime),
          Layer.provide(Layer.succeed(SqlClient.SqlClient, gate.sql)),
          Layer.provide(NodeCrypto.layer),
          Layer.build,
          Effect.map(Context.get(ServerStore.ServerStore))
        )
        yield* pool`INSERT INTO effect_local_server_offline_wakes
          (space_id, client_id, wake_id, high_water_sequence, notified_sequence, membership_generation,
            attempt_count, next_attempt_at, claim_token, claimed_until)
          VALUES (${spaceId}, ${claimedId}, 'wak_00000000-0000-4000-8000-000000000a01', 1, 0, 1, 0, 0,
            'held-by-another-runtime', 1000000000000)`

        const claimedWakes = yield* Queue.unbounded<Protocol.Wake>()
        yield* store.watch(watchRequest(claimedId)).pipe(
          Stream.runForEach((wake) => Queue.offer(claimedWakes, wake)),
          Effect.forkChild({ startImmediately: true })
        )
        const blocked = yield* Queue.take(gate.pauses)
        assert.deepStrictEqual(blocked.rows, [])
        yield* Deferred.succeed(blocked.release, undefined)
        yield* Queue.take(gate.pauses).pipe(
          Effect.flatMap((pause) => Deferred.succeed(pause.release, undefined)),
          Effect.forever,
          Effect.forkChild
        )

        const opened = yield* store.watch(watchRequest(otherId)).pipe(Stream.take(1), Stream.runCollect)
        assert.strictEqual(opened.length, 1)
        assert.strictEqual(yield* Queue.size(claimedWakes), 0)
      },
      Effect.scoped,
      provideReactivity
    )
  )
})
