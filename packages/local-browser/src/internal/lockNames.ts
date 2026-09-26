export interface LockNames {
  readonly runner: (host: string) => string
  readonly runnerPrefix: string
  readonly ready: (host: string) => string
  readonly readyPrefix: string
  readonly machine: (id: number) => string
  readonly leader: string
  readonly clientIdentity: string
}

export const make = (name: string): LockNames => {
  const base = `@lucas-barake/effect-local-browser:${name}`
  const runnerPrefix = `${base}:runner:`
  const readyPrefix = `${base}:ready:`
  return {
    runner: (host) => `${runnerPrefix}${host}`,
    runnerPrefix,
    ready: (host) => `${readyPrefix}${host}`,
    readyPrefix,
    machine: (id) => `${base}:machine:${id}`,
    leader: `${base}:leader`,
    clientIdentity: `${base}:client-identity`
  }
}
