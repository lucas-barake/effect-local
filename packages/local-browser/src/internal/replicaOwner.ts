import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import type * as BuildGate from "./buildGate.js"
import * as InvalidationHub from "./invalidationHub.js"
import type * as lockNames from "./lockNames.js"
import * as LosslessQueue from "./losslessQueue.js"
import type * as platform from "./platform.js"
import type { OwnerResources } from "./replicaHost.js"

export interface Options {
  readonly host: string
  readonly names: lockNames.LockNames
  readonly locks: platform.WebLocksService
  readonly channels: platform.TabChannelService
  readonly visibility: platform.TabVisibilityService
  readonly retryDelay: Duration.Input
  readonly gate: BuildGate.BuildGate
}

export interface ReplicaOwner {
  readonly lease: Effect.Effect<OwnerResources, never, Scope.Scope>
  readonly drainingLease: Effect.Effect<OwnerResources, never, Scope.Scope>
  readonly isDraining: () => boolean
}

interface Holder {
  readonly fiber: Fiber.Fiber<unknown, unknown>
  readonly interruptOnFence: boolean
}

interface Term {
  readonly resources: OwnerResources
  readonly drained: Deferred.Deferred<void>
  readonly holders: Set<Holder>
  fenced: boolean
}

const invalidationBacklogCapacity = 16_384

const stringKeys = (keys: ReadonlyArray<unknown> | Readonly<Record<string, ReadonlyArray<unknown>>>) => {
  const flat: Array<string> = []
  if (Array.isArray(keys)) {
    for (const key of keys) {
      if (typeof key === "string") flat.push(key)
    }
    return flat
  }
  for (const key of Object.keys(keys)) flat.push(key)
  return flat
}

const decodeAnnouncement = Schema.decodeUnknownEffect(Schema.String)

export const make = Effect.fnUntraced(function*<E extends { readonly _tag: string }, R,>(
  layerOwner: Layer.Layer<
    Replica.Replica | QueryReactivity.QueryReactivity | EphemeralClient.EphemeralClient,
    E,
    Reactivity.Reactivity | R
  >,
  options: Options
) {
  const ownerScope = yield* Effect.scope
  const names = options.names
  let terms = 0
  let active: Term | undefined
  const runners = yield* options.channels.open(names.runnersChannel)
  const announceRunners = runners.post(options.host)
  let draining = false

  const settleDrain = (term: Term) => {
    if (term.fenced && term.holders.size === 0) Deferred.doneUnsafe(term.drained, Effect.void)
  }

  const fence = (term: Term) =>
    Effect.suspend(() => {
      term.fenced = true
      if (active === term) active = undefined
      const fibers: Array<Fiber.Fiber<unknown, unknown>> = []
      for (const holder of term.holders) {
        if (holder.interruptOnFence) fibers.push(holder.fiber)
      }
      settleDrain(term)
      return Fiber.interruptAll(fibers)
    })

  const leaseWith = (interruptOnFence: boolean) =>
    Effect.acquireRelease(
      Effect.withFiber((fiber) => {
        const term = active
        if (term === undefined || term.fenced) return Effect.interrupt
        const holder: Holder = { fiber, interruptOnFence }
        term.holders.add(holder)
        return Effect.succeed({ term, holder })
      }),
      ({ holder, term }) =>
        Effect.sync(() => {
          term.holders.delete(holder)
          settleDrain(term)
        })
    ).pipe(Effect.map(({ term }) => term.resources))

  const visible = yield* SubscriptionRef.make(yield* options.visibility.visible)
  const announcements = yield* options.channels.open(names.visibilityChannel)
  let marker: Scope.Closeable | undefined

  const releaseMarker = Effect.suspend(() => {
    if (marker === undefined) return Effect.void
    const released = marker
    marker = undefined
    return Scope.close(released, Exit.void)
  })

  const syncMarker = Effect.gen(function*() {
    const now = yield* options.visibility.visible
    yield* SubscriptionRef.set(visible, now)
    if (now && marker === undefined) {
      const markerScope = yield* Scope.fork(ownerScope)
      marker = markerScope
      yield* options.locks.acquire(names.visible(options.host)).pipe(Scope.provide(markerScope))
      yield* announcements.post(options.host)
    } else if (!now) {
      yield* releaseMarker
    }
  })

  const superseded = Deferred.await(options.gate.superseded).pipe(Effect.ignore)

  yield* options.visibility.changes.pipe(
    Stream.runForEach(() => syncMarker),
    Effect.raceFirst(superseded),
    Effect.andThen(releaseMarker),
    Effect.forkScoped
  )

  const visibleElsewhere = options.locks.held.pipe(
    Effect.map((held) => {
      const hosts: Array<string> = []
      for (const name of held) {
        if (!name.startsWith(names.visiblePrefix)) continue
        const host = name.slice(names.visiblePrefix.length)
        if (host !== options.host) hosts.push(host)
      }
      return hosts
    })
  )

  const outranked = Effect.gen(function*() {
    if (yield* SubscriptionRef.get(visible)) return false
    const hosts = yield* visibleElsewhere
    return hosts.length > 0
  })

  const awaitCandidacy = Effect.gen(function*() {
    while (true) {
      if (yield* SubscriptionRef.get(visible)) return
      const hosts = yield* visibleElsewhere
      if (hosts.length === 0) return
      const hidden = hosts.map((host) => options.locks.released(names.visible(host)))
      yield* Effect.raceFirst(
        SubscriptionRef.changes(visible).pipe(Stream.filter((now) => now), Stream.runHead),
        Effect.raceAll(hidden)
      )
    }
  })

  const awaitOutranked = Effect.gen(function*() {
    const inbox = yield* announcements.messages
    const announced = LosslessQueue.stream(inbox).pipe(
      Stream.mapEffect((raw) =>
        decodeAnnouncement(raw).pipe(
          Effect.as(true),
          Effect.catchTag("SchemaError", () => Effect.succeed(false))
        )
      ),
      Stream.filter((valid) => valid)
    )
    yield* LosslessQueue.merge(SubscriptionRef.changes(visible), announced).pipe(
      Stream.mapEffect(() => outranked),
      Stream.filter((yieldLeadership) => yieldLeadership),
      Stream.runHead
    )
  })

  const lead = Effect.gen(function*() {
    yield* awaitCandidacy
    yield* options.locks.acquire(names.leader)
    if ((yield* options.gate.check) || (yield* outranked)) return
    const invalidations = yield* Effect.acquireRelease(
      Effect.sync(() => InvalidationHub.make(invalidationBacklogCapacity)),
      (hub) => hub.shutdown
    )
    const base = yield* Reactivity.make
    const reactivity: Reactivity.Reactivity = {
      ...base,
      invalidate: (keys) =>
        base.invalidate(keys).pipe(
          Effect.andThen(Effect.suspend(() => {
            return invalidations.publish(stringKeys(keys))
          }))
        )
    }
    const context = yield* Layer.build(
      layerOwner.pipe(Layer.provide(Layer.succeed(Reactivity.Reactivity, reactivity)))
    )
    terms += 1
    const resources: OwnerResources = {
      session: `${options.host}:${terms}`,
      replica: Context.get(context, Replica.Replica),
      queryReactivity: Context.get(context, QueryReactivity.QueryReactivity),
      ephemeral: Context.get(context, EphemeralClient.EphemeralClient),
      invalidations
    }
    const term: Term = { resources, drained: yield* Deferred.make<void>(), holders: new Set(), fenced: false }
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        draining = true
      }).pipe(
        Effect.andThen(fence(term)),
        Effect.andThen(Deferred.await(term.drained)),
        Effect.andThen(Effect.sync(() => {
          draining = false
        })),
        Effect.andThen(announceRunners)
      )
    )
    active = term
    const readyScope = yield* Scope.fork(yield* Effect.scope)
    yield* options.locks.acquire(names.ready(options.host)).pipe(Scope.provide(readyScope))
    yield* announceRunners
    yield* awaitOutranked
    draining = true
    yield* Scope.close(readyScope, Exit.void)
    yield* options.locks.released(names.ready(options.host))
    yield* announceRunners
  })

  yield* Effect.scoped(lead).pipe(
    Effect.tapCause((cause) =>
      Effect.logWarning("browser replica owner stack stopped").pipe(Effect.annotateLogs({ cause: String(cause) }))
    ),
    Effect.exit,
    Effect.flatMap(Exit.match({
      onSuccess: () => Effect.void,
      onFailure: () => Effect.sleep(options.retryDelay)
    })),
    Effect.forever,
    Effect.raceFirst(superseded),
    Effect.forkScoped
  )

  const owner: ReplicaOwner = {
    lease: leaseWith(true),
    drainingLease: leaseWith(false),
    isDraining: () => draining
  }
  return owner
})
