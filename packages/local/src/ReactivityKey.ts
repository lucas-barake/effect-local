import * as Canonical from "./Canonical.js"

export const entity = (model: string, key: unknown): string => `effect-local:entity:${Canonical.hash({ model, key })}`

export const model = (name: string): string => `effect-local:model:${Canonical.hash(name)}`

export const query = (name: string, payload: unknown): string =>
  `effect-local:query:${Canonical.hash({ name, payload })}`
