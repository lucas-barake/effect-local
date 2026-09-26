import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import { PersistenceError } from "effect/unstable/cluster/ClusterError"
import * as MachineId from "effect/unstable/cluster/MachineId"
import * as Runner from "effect/unstable/cluster/Runner"
import * as RunnerAddress from "effect/unstable/cluster/RunnerAddress"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import type * as ShardId from "effect/unstable/cluster/ShardId"
import type * as lockNames from "./lockNames.js"
import type * as platform from "./platform.js"

const machineIdCount = 1024

export interface Options {
  readonly self: RunnerAddress.RunnerAddress
  readonly names: lockNames.LockNames
  readonly locks: platform.WebLocksService
  readonly groups: ReadonlyArray<string>
  readonly weight: number
  readonly isReady: () => boolean
}

export interface TabRunnerStorage {
  readonly storage: RunnerStorage.RunnerStorage["Service"]
  readonly registered: Effect.Effect<void>
}

export const make = Effect.fnUntraced(function*(options: Options) {
  const storageScope = yield* Effect.scope
  const registeredLatch = yield* Deferred.make<void>()
  const self = options.self
  let machineId: MachineId.MachineId | undefined
  let registration: Scope.Closeable | undefined

  const claimMachineId = Effect.gen(function*() {
    for (let candidate = 0; candidate < machineIdCount; candidate++) {
      const hold = yield* options.locks.tryAcquire(options.names.machine(candidate)).pipe(
        Scope.provide(storageScope)
      )
      if (Option.isSome(hold)) return MachineId.make(candidate)
    }
    return yield* new PersistenceError({ cause: `All ${machineIdCount} tab machine ids are held` })
  })

  const hostOf = (prefix: string, name: string) => name.slice(prefix.length)

  const register = Effect.gen(function*() {
    if (machineId === undefined) machineId = yield* claimMachineId
    if (registration === undefined) {
      const scope = yield* Scope.fork(storageScope)
      yield* options.locks.acquire(options.names.runner(self.host)).pipe(Scope.provide(scope))
      registration = scope
    }
    yield* Deferred.succeed(registeredLatch, undefined)
    return machineId
  })

  const ownedShards = (address: RunnerAddress.RunnerAddress, shardIds: Iterable<ShardId.ShardId>) =>
    Effect.sync(() => {
      if (!ownsShards(address)) return []
      return Array.from(shardIds)
    })

  const ownsShards = (address: RunnerAddress.RunnerAddress) => address.host === self.host && options.isReady()

  const storage = RunnerStorage.RunnerStorage.of({
    register: () => register,
    unregister: (address) =>
      Effect.suspend(() => {
        const scope = registration
        if (address.host !== self.host || scope === undefined) return Effect.void
        registration = undefined
        return Scope.close(scope, Exit.void)
      }),
    getRunners: Effect.gen(function*() {
      const held = yield* options.locks.held
      const ready = new Set<string>()
      const hosts: Array<string> = []
      for (const name of held) {
        if (name.startsWith(options.names.readyPrefix)) ready.add(hostOf(options.names.readyPrefix, name))
        else if (name.startsWith(options.names.runnerPrefix)) hosts.push(hostOf(options.names.runnerPrefix, name))
      }
      return hosts.map((host) => {
        const runner = Runner.make({
          address: RunnerAddress.make(host, self.port),
          groups: options.groups,
          weight: options.weight
        })
        return [runner, ready.has(host)] as const
      })
    }),
    setRunnerHealth: () => Effect.void,
    acquire: ownedShards,
    refresh: ownedShards,
    release: () => Effect.void,
    releaseAll: () => Effect.void
  })

  const result: TabRunnerStorage = { storage, registered: Deferred.await(registeredLatch) }
  return result
})
