import * as Canonical from "./Canonical.js"
import type * as Identity from "./Identity.js"

export const spaces = "effect-local:spaces"

export const aggregateStatus = "effect-local:aggregate-status"

export const membership = (spaceId: Identity.SpaceId): string => `effect-local:space:${spaceId}`

export const scope = (spaceId: Identity.SpaceId): string => `${membership(spaceId)}:scope`

export const activation = (spaceId: Identity.SpaceId): string => `${membership(spaceId)}:activation`

export const status = (spaceId: Identity.SpaceId): string => `${membership(spaceId)}:status`

export const entity = (spaceId: Identity.SpaceId, model: string, key: unknown): string =>
  `effect-local:entity:${Canonical.hash({ spaceId, model, key })}`

export const query = (spaceId: Identity.SpaceId, name: string, payload: unknown): string =>
  `effect-local:query:${Canonical.hash({ spaceId, name, payload })}`

export const receipt = (spaceId: Identity.SpaceId, mutationId: Identity.MutationId): string =>
  `${membership(spaceId)}:receipt:${mutationId}`

export const pending = (spaceId: Identity.SpaceId): string => `${membership(spaceId)}:pending`
