export interface LockNames {
  readonly runner: (host: string) => string
  readonly runnerPrefix: string
  readonly ready: (host: string) => string
  readonly readyPrefix: string
  readonly visible: (host: string) => string
  readonly visiblePrefix: string
  readonly visibilityChannel: string
  readonly machine: (id: number) => string
  readonly leader: string
  readonly clientIdentity: string
}

export const make = (name: string): LockNames => {
  const base = `@lucas-barake/effect-local-browser:${name}`
  const runnerPrefix = `${base}:runner:`
  const readyPrefix = `${base}:ready:`
  const visiblePrefix = `${base}:visible:`
  return {
    runner: (host) => `${runnerPrefix}${host}`,
    runnerPrefix,
    ready: (host) => `${readyPrefix}${host}`,
    readyPrefix,
    visible: (host) => `${visiblePrefix}${host}`,
    visiblePrefix,
    visibilityChannel: `${base}:visibility`,
    machine: (id) => `${base}:machine:${id}`,
    leader: `${base}:leader`,
    clientIdentity: `${base}:client-identity`
  }
}
