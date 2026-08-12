import * as Canonical from "./Canonical.js"
import type * as Identity from "./Identity.js"

export const entity = (spaceId: Identity.SpaceId, model: string, key: unknown): string =>
  `effect-local:entity:${Canonical.hash({ spaceId, model, key })}`

export const query = (spaceId: Identity.SpaceId, name: string, payload: unknown): string =>
  `effect-local:query:${Canonical.hash({ spaceId, name, payload })}`
