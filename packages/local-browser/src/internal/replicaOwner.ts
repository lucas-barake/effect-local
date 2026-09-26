import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as InvalidationHub from "./invalidationHub.js"
import type * as lockNames from "./lockNames.js"
import type * as platform from "./platform.js"
import type { OwnerResources } from "./replicaHost.js"

export interface Options<E extends { readonly _tag: string },> {
  readonly host: string
  readonly names: lockNames.LockNames
  readonly locks: platform.WebLocksService
  readonly channels: platform.TabChannelService
  readonly visibility: platform.TabVisibilityService
  readonly shardsReleased: Effect.Effect<void>
  readonly retryDelay: Duration.Input
  readonly layerOwner: Layer.Layer<
    Replica.Replica | QueryReactivity.QueryReactivity | EphemeralClient.EphemeralClient,
    E,
    Reactivity.Reactivity
  >
}

export interface ReplicaOwner {
  readonly current: Effect.Effect<OwnerResources>
  readonly isReady: () => boolean
}

export class ReplicaOwnerService extends Context.Service<ReplicaOwnerService, ReplicaOwner>()(
  "@lucas-barake/effect-local-browser/ReplicaOwner"
) {}

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

export const make = Effect.fnUntraced(function*<E extends { readonly _tag: string },>(options: Options<E>) {
  const ownerScope = yield* Effect.scope
  const names = options.names
  let ready = false
  let terms = 0
  let term = yield* Deferred.make<OwnerResources>()

  const visible = yield* SubscriptionRef.make(yield* options.visibility.visible)
  const announcements = yield* options.channels.open(names.visibilityChannel)
  let marker: Scope.Closeable | undefined

  const syncMarker = Effect.gen(function*() {
    const now = yield* options.visibility.visible
    yield* SubscriptionRef.set(visible, now)
    if (now && marker === undefined) {
      const markerScope = yield* Scope.fork(ownerScope)
      marker = markerScope
      yield* options.locks.acquire(names.visible(options.host)).pipe(Scope.provide(markerScope))
      yield* announcements.post(options.host)
    } else if (!now && marker !== undefined) {
      const released = marker
      marker = undefined
      yield* Scope.close(released, Exit.void)
    }
  })

  yield* options.visibility.changes.pipe(
    Stream.runForEach(() => syncMarker),
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
    const announced = Stream.fromQueue(inbox).pipe(
      Stream.mapEffect((raw) =>
        decodeAnnouncement(raw).pipe(
          Effect.as(true),
          Effect.catchTag("SchemaError", () => Effect.succeed(false))
        )
      ),
      Stream.filter((valid) => valid)
    )
    yield* Stream.merge(SubscriptionRef.changes(visible), announced).pipe(
      Stream.mapEffect(() => outranked),
      Stream.filter((yieldLeadership) => yieldLeadership),
      Stream.runHead
    )
  })

  const lead = Effect.gen(function*() {
    yield* awaitCandidacy
    yield* options.locks.acquire(names.leader)
    if (yield* outranked) return
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
      options.layerOwner.pipe(Layer.provide(Layer.succeed(Reactivity.Reactivity, reactivity)))
    )
    terms += 1
    const resources: OwnerResources = {
      session: `${options.host}:${terms}`,
      replica: Context.get(context, Replica.Replica),
      queryReactivity: Context.get(context, QueryReactivity.QueryReactivity),
      ephemeral: Context.get(context, EphemeralClient.EphemeralClient),
      invalidations
    }
    yield* Effect.addFinalizer(() =>
      Deferred.make<OwnerResources>().pipe(
        Effect.map((next) => {
          ready = false
          term = next
        })
      )
    )
    yield* Deferred.succeed(term, resources)
    const readyScope = yield* Scope.fork(yield* Effect.scope)
    yield* options.locks.acquire(names.ready(options.host)).pipe(Scope.provide(readyScope))
    ready = true
    yield* awaitOutranked
    ready = false
    yield* Scope.close(readyScope, Exit.void)
    yield* options.shardsReleased
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
    Effect.forkScoped
  )

  const owner: ReplicaOwner = {
    current: Effect.suspend(() => Deferred.await(term)),
    isReady: () => ready
  }
  return owner
})
