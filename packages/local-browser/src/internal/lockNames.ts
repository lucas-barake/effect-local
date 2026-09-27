export interface Presence {
  readonly sequence: number
  readonly version: number
  readonly fingerprint: string
  readonly host: string
}

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
  readonly presence: (presence: Presence) => string
  readonly presencePrefix: string
  readonly presenceChannel: string
}

export const make = (name: string, fingerprint: string): LockNames => {
  const base = `@lucas-barake/effect-local-browser:${encodeURIComponent(name)}`
  const build = `${base}:build:${fingerprint}`
  const runnerPrefix = `${build}:runner:`
  const readyPrefix = `${build}:ready:`
  const visiblePrefix = `${build}:visible:`
  const presencePrefix = `${base}:presence:`
  return {
    runner: (host) => `${runnerPrefix}${host}`,
    runnerPrefix,
    ready: (host) => `${readyPrefix}${host}`,
    readyPrefix,
    visible: (host) => `${visiblePrefix}${host}`,
    visiblePrefix,
    visibilityChannel: `${build}:visibility`,
    runnersChannel: `${build}:runners`,
    inbox: (host) => `${build}:inbox:${host}`,
    machine: (id) => `${build}:machine:${id}`,
    leader: `${base}:leader`,
    clientIdentity: `${base}:client-identity`,
    presence: (presence) =>
      `${presencePrefix}${presence.sequence}:${presence.version}:${presence.fingerprint}:${presence.host}`,
    presencePrefix,
    presenceChannel: `${base}:presence`
  }
}
