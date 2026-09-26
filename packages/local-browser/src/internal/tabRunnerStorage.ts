import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { PersistenceError } from "effect/unstable/cluster/ClusterError"
import * as MachineId from "effect/unstable/cluster/MachineId"
import * as Runner from "effect/unstable/cluster/Runner"
import * as RunnerAddress from "effect/unstable/cluster/RunnerAddress"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import type * as ShardId from "effect/unstable/cluster/ShardId"
import type * as lockNames from "./lockNames.js"
import type * as platform from "./platform.js"

const machineIdCount = 1024

const runnersWaitCap = Duration.seconds(2)

export interface Options {
  readonly self: RunnerAddress.RunnerAddress
  readonly names: lockNames.LockNames
  readonly locks: platform.WebLocksService
  readonly channels: platform.TabChannelService
  readonly groups: ReadonlyArray<string>
  readonly weight: number
  readonly isDraining: () => boolean
}

export interface TabRunnerStorage {
  readonly storage: RunnerStorage.RunnerStorage["Service"]
  readonly registered: Effect.Effect<void>
  readonly awaitRouted: Effect.Effect<boolean>
}

interface Snapshot {
  readonly runners: Array<readonly [Runner.Runner, boolean]>
  readonly signature: string
  readonly others: ReadonlyArray<string>
  readonly othersReady: ReadonlyArray<string>
  readonly hasReady: boolean
}

interface Delivery {
  readonly sequence: number
  readonly hasReady: boolean
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
  const names = options.names
  const runnersChannel = yield* options.channels.open(names.runnersChannel)
  const nudges = yield* runnersChannel.messages
  const wakes = yield* Queue.unbounded<void>()
  let returned: Delivery & { readonly signature: string | undefined } = {
    sequence: 0,
    hasReady: false,
    signature: undefined
  }
  const processed = yield* SubscriptionRef.make<Delivery>({ sequence: 0, hasReady: false })

  const snapshot = options.locks.held.pipe(
    Effect.map((held): Snapshot => {
      const ready = new Set<string>()
      const hosts: Array<string> = []
      for (const name of held) {
        if (name.startsWith(names.readyPrefix)) ready.add(hostOf(names.readyPrefix, name))
        else if (name.startsWith(names.runnerPrefix)) hosts.push(hostOf(names.runnerPrefix, name))
      }
      hosts.sort()
      const healthy = (host: string) => ready.has(host) || (host === self.host && options.isDraining())
      const runners = hosts.map((host) => {
        const runner = Runner.make({
          address: RunnerAddress.make(host, self.port),
          groups: options.groups,
          weight: options.weight
        })
        return [runner, healthy(host)] as const
      })
      const others = hosts.filter((host) => host !== self.host)
      return {
        runners,
        signature: hosts.map((host) => `${host}:${Number(healthy(host))}`).join(","),
        others,
        othersReady: others.filter((host) => ready.has(host)),
        hasReady: hosts.some((host) => ready.has(host))
      }
    })
  )

  const awaitChange = (current: Snapshot) =>
    Effect.raceAll([
      Queue.take(nudges).pipe(Effect.asVoid),
      Queue.take(wakes),
      Effect.sleep(runnersWaitCap),
      ...current.others.map((host) => options.locks.released(names.runner(host))),
      ...current.othersReady.map((host) => options.locks.released(names.ready(host)))
    ]).pipe(
      Effect.andThen(Queue.clear(nudges)),
      Effect.andThen(Queue.clear(wakes)),
      Effect.asVoid
    )

  const getRunners = Effect.gen(function*() {
    yield* SubscriptionRef.set(processed, { sequence: returned.sequence, hasReady: returned.hasReady })
    let current = yield* snapshot
    if (current.signature === returned.signature) {
      yield* awaitChange(current)
      current = yield* snapshot
    }
    returned = { sequence: returned.sequence + 1, hasReady: current.hasReady, signature: current.signature }
    return current.runners
  })

  const closed = yield* Deferred.make<void>()
  yield* Effect.addFinalizer(() => Deferred.succeed(closed, undefined))

  const awaitRouted = Effect.suspend(() => {
    const target = returned.sequence + 1
    const routed = Queue.offer(wakes, undefined).pipe(
      Effect.andThen(
        SubscriptionRef.changes(processed).pipe(
          Stream.filter((delivery) => delivery.sequence >= target && delivery.hasReady),
          Stream.runHead
        )
      ),
      Effect.as(true)
    )
    return Effect.raceFirst(Deferred.await(closed).pipe(Effect.as(false)), routed)
  })

  const register = Effect.gen(function*() {
    if (machineId === undefined) machineId = yield* claimMachineId
    if (registration === undefined) {
      const scope = yield* Scope.fork(storageScope)
      yield* options.locks.acquire(options.names.runner(self.host)).pipe(Scope.provide(scope))
      registration = scope
      yield* runnersChannel.post(self.host)
    }
    yield* Deferred.succeed(registeredLatch, undefined)
    return machineId
  })

  const ownedShards = (address: RunnerAddress.RunnerAddress, shardIds: Iterable<ShardId.ShardId>) =>
    Effect.sync(() => {
      if (address.host !== self.host) return []
      return Array.from(shardIds)
    })

  const storage = RunnerStorage.RunnerStorage.of({
    register: () => register,
    unregister: (address) =>
      Effect.suspend(() => {
        const scope = registration
        if (address.host !== self.host || scope === undefined) return Effect.void
        registration = undefined
        const released = options.locks.released(names.runner(self.host))
        return Scope.close(scope, Exit.void).pipe(
          Effect.andThen(released),
          Effect.andThen(runnersChannel.post(self.host))
        )
      }),
    getRunners,
    setRunnerHealth: () => Effect.void,
    acquire: ownedShards,
    refresh: ownedShards,
    release: () => Effect.void,
    releaseAll: () => Effect.void
  })

  const result: TabRunnerStorage = { storage, registered: Deferred.await(registeredLatch), awaitRouted }
  return result
})
