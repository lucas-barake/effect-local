import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Duration from "effect/Duration"
import type * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"

type HookRejection = Schema.JsonObject & { readonly _tag: string }

export interface Delivery {
  readonly wakeId: Identity.WakeId
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
}

export type DeliveryOutcome = "Delivered" | "NotRecipient"

export interface Options<R = never,> {
  readonly recipients: (input: {
    readonly spaceId: Identity.SpaceId
  }) => Effect.Effect<ReadonlyArray<Identity.ClientId>, HookRejection, R>
  readonly deliver: (wake: Delivery) => Effect.Effect<DeliveryOutcome, HookRejection, R>
  readonly coalescingWindow: Duration.Input
  readonly pollInterval: Duration.Input
  readonly retryDelay: Duration.Input
  readonly maximumRetryDelay: Duration.Input
  readonly claimLeaseDuration: Duration.Input
  readonly hookTimeout: Duration.Input
  readonly presenceLeaseDuration: Duration.Input
  readonly presenceHeartbeatInterval: Duration.Input
  readonly claimBatchSize: number
  readonly maximumConcurrentRecipientResolutions: number
  readonly maximumConcurrentDeliveries: number
  readonly maximumRecipientsPerSpace: number
}
