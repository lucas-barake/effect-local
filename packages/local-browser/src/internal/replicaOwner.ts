import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import * as Schedule from "effect/Schedule"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type * as lockNames from "./lockNames.js"
import type * as platform from "./platform.js"
import type { OwnerResources } from "./replicaHost.js"

export interface Options<E extends { readonly _tag: string },> {
  readonly host: string
  readonly names: lockNames.LockNames
  readonly locks: platform.WebLocksService
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

export const make = Effect.fnUntraced(function*<E extends { readonly _tag: string },>(options: Options<E>) {
  let ready = false
  let terms = 0
  let term = yield* Deferred.make<OwnerResources>()

  const lead = Effect.gen(function*() {
    yield* options.locks.acquire(options.names.leader)
    const invalidations = yield* Effect.acquireRelease(
      PubSub.unbounded<ReadonlyArray<string>>(),
      PubSub.shutdown
    )
    const base = yield* Reactivity.make
    const reactivity: Reactivity.Reactivity = {
      ...base,
      invalidate: (keys) =>
        base.invalidate(keys).pipe(
          Effect.andThen(Effect.suspend(() => {
            const flat = stringKeys(keys)
            if (flat.length === 0) return Effect.void
            return PubSub.publish(invalidations, flat)
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
    yield* options.locks.acquire(options.names.ready(options.host))
    ready = true
    return yield* Effect.never
  })

  yield* Effect.scoped(lead).pipe(
    Effect.tapCause((cause) =>
      Effect.logWarning("browser replica owner stack stopped").pipe(Effect.annotateLogs({ cause: String(cause) }))
    ),
    Effect.exit,
    Effect.repeat(Schedule.spaced(options.retryDelay)),
    Effect.forkScoped
  )

  const owner: ReplicaOwner = {
    current: Effect.suspend(() => Deferred.await(term)),
    isReady: () => ready
  }
  return owner
})
