import { assert, describe, it } from "@effect/vitest"
import type * as Effect from "effect/Effect"
import type * as Layer from "effect/Layer"
import type * as CommandDeliveryStore from "../src/CommandDeliveryStore.js"
import * as PublicApi from "../src/index.js"
import type * as PeerSession from "../src/PeerSession.js"
import type * as SqlReplica from "../src/SqlReplica.js"

type Requirements<T,> = T extends Effect.Effect<unknown, unknown, infer R> ? R : never

describe("public SQL API types", () => {
  it("exports the opt in relay persistence services", () => {
    const publicApi: Record<string, unknown> = PublicApi

    for (
      const name of [
        "PeerRelayClientRuntime",
        "PeerRelayOutbox",
        "PeerRelayOutboxLimits",
        "PeerRelayReceiptLimits",
        "PeerConnectionStatus",
        "RelayConnectionStatus",
        "PeerSyncEnvelope"
      ]
    ) {
      assert.property(publicApi, name)
    }
  })

  it("requires command delivery storage when constructing a peer session", () => {
    type SessionRequirements = Requirements<ReturnType<typeof PeerSession.make>>
    const requiresDeliveryStore: CommandDeliveryStore.CommandDeliveryStore extends SessionRequirements ? true
      : false = true
    assert.isTrue(requiresDeliveryStore)
  })

  it("retains command delivery storage in the SQL replica layer", () => {
    type ReplicaServices = Layer.Success<ReturnType<typeof SqlReplica.layer>>
    const providesDeliveryStore: CommandDeliveryStore.CommandDeliveryStore extends ReplicaServices ? true
      : false = true
    assert.isTrue(providesDeliveryStore)
  })
})
