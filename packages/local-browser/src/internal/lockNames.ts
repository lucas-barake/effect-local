export interface LockNames {
  readonly runner: (host: string) => string
  readonly runnerPrefix: string
  readonly ready: (host: string) => string
  readonly readyPrefix: string
  readonly visible: (host: string) => string
  readonly visiblePrefix: string
  readonly visibilityChannel: string
  readonly runnersChannel: string
  readonly inbox: (host: string) => string
  readonly machine: (id: number) => string
  readonly leader: string
  readonly clientIdentity: string
}

export const make = (name: string): LockNames => {
  const base = `@lucas-barake/effect-local-browser:${encodeURIComponent(name)}`
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
    runnersChannel: `${base}:runners`,
    inbox: (host) => `${base}:inbox:${host}`,
    machine: (id) => `${base}:machine:${id}`,
    leader: `${base}:leader`,
    clientIdentity: `${base}:client-identity`
  }
}
