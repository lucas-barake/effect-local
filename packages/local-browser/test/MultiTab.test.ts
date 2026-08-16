import { assert, describe, it } from "@effect/vitest"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as broadcastRpc from "../src/internal/broadcastRpc.js"
import * as Wire from "../src/internal/multiTabWire.js"
import * as platform from "../src/internal/platform.js"
import * as replicaWire from "../src/internal/replicaWire.js"
import * as MultiTab from "../src/MultiTab.js"
import * as testKit from "./multiTabKit.js"

const definition = Definition.make({ version: 1, models: [], mutations: [] })

const inertReplica: Replica.Service = {
  join: () => Effect.never,
  leave: () => Effect.never,
  spaces: Effect.succeed([]),
  space: () => Effect.never,
  status: Effect.never
}

const inertSpace = (spaceId: Identity.SpaceId): Replica.Space => ({
  spaceId,
  scope: Effect.succeed(Protocol.ReplicationScope.make({ models: [] })),
  setScope: () => Effect.never,
  activation: Effect.never,
  activate: Effect.never,
  deactivate: Effect.never,
  mutate: () => Effect.never,
  get: () => Effect.never,
  query: () => Effect.never,
  receipt: () => Effect.never,
  pending: Effect.never,
  pendingFor: () => Effect.never,
  settlements: () => Stream.never,
  settlementsFor: () => Stream.never,
  acknowledgeSettlements: () => Effect.never,
  quarantine: Effect.never,
  discardQuarantined: () => Effect.never,
  resubmitQuarantined: () => Effect.never,
  status: Effect.succeed({ _tag: "Offline", spaceId, pending: 0 })
})

const inertQueryReactivity: QueryReactivity.Service = {
  retain: () => Effect.succeed(Effect.void),
  record: () => Effect.void,
  affected: () => Effect.succeed([])
}

const inertEphemeral: EphemeralClient.Service = {
  session: () => Effect.never,
  publish: () => Effect.never,
  clear: () => Effect.never,
  remove: () => Effect.never
}

const layerOwner = Layer.mergeAll(
  Layer.succeed(Replica.Replica, inertReplica),
  Layer.succeed(QueryReactivity.QueryReactivity, inertQueryReactivity),
  Layer.succeed(EphemeralClient.EphemeralClient, inertEphemeral)
)

const multiTab = (
  options: Omit<MultiTab.Options<never>, "platform">,
  layerPlatform: NonNullable<MultiTab.Options<never>["platform"]>
) =>
  MultiTab.layer({
    ...options,
    // oxlint-disable-next-line effect-local/requireLayerName -- The public option is named platform; its value uses the required Layer naming convention.
    platform: layerPlatform
  })

describe("MultiTab", () => {
  it.effect(
    "preserves one client identity across concurrent startup and takeover",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const stored = new Map<string, string>()
      const firstStoreStarted = yield* Deferred.make<void>()
      const allowFirstStore = yield* Deferred.make<void>()
      const secondIdentityLockStarted = yield* Deferred.make<void>()
      let stores = 0
      let identityLockAttempts = 0
      const identities: platform.ClientIdentityStoreService = {
        load: (key) => Effect.sync(() => stored.get(key)),
        store: Effect.fnUntraced(function*(key, value) {
          stores += 1
          if (stores === 1) {
            yield* Deferred.succeed(firstStoreStarted, undefined)
            yield* Deferred.await(allowFirstStore)
          }
          stored.set(key, value)
        })
      }
      const layerPlatform = Layer.mergeAll(
        Layer.succeed(platform.TabChannel, kit.tabChannel),
        Layer.succeed(platform.WebLocks, {
          acquire: (name, options) => {
            if (!name.endsWith(":client-identity")) return kit.webLocks.acquire(name, options)
            identityLockAttempts += 1
            let started = Effect.void
            if (identityLockAttempts === 2) {
              started = Deferred.succeed(secondIdentityLockStarted, undefined)
            }
            return started.pipe(Effect.andThen(kit.webLocks.acquire(name, options)))
          }
        }),
        Layer.succeed(platform.EpochStore, kit.epochStore),
        Layer.succeed(platform.ClientIdentityStore, identities)
      )
      const owners = yield* Queue.make<{ readonly tab: string; readonly clientId: string }>()
      const owner = (tab: string) => ({ clientId }: MultiTab.OwnerContext) =>
        Layer.effectContext(
          Queue.offer(owners, { tab, clientId }).pipe(
            Effect.as(
              Context.empty().pipe(
                Context.add(Replica.Replica, inertReplica),
                Context.add(QueryReactivity.QueryReactivity, inertQueryReactivity),
                Context.add(EphemeralClient.EphemeralClient, inertEphemeral)
              )
            )
          )
        )
      const options = (tab: string) => ({
        name: "identity-race",
        definition,
        owner: owner(tab),
        requestPersistence: false
      })
      const scopeA = yield* Scope.make()
      const scopeB = yield* Scope.make()
      const layerA = multiTab(options("a"), layerPlatform)
      const layerB = multiTab(options("b"), layerPlatform)
      const buildA = yield* Scope.provide(
        Layer.build(layerA).pipe(Effect.provide(Reactivity.layer)),
        scopeA
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(firstStoreStarted)
      const buildB = yield* Scope.provide(
        Layer.build(layerB).pipe(Effect.provide(Reactivity.layer)),
        scopeB
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(secondIdentityLockStarted)
      yield* Deferred.succeed(allowFirstStore, undefined)
      const first = yield* Queue.take(owners)
      let firstScope = scopeB
      let secondScope = scopeA
      if (first.tab === "a") {
        firstScope = scopeA
        secondScope = scopeB
      }
      yield* Scope.close(firstScope, Exit.void)
      const second = yield* Queue.take(owners)
      assert.strictEqual(second.clientId, first.clientId)
      yield* Scope.close(secondScope, Exit.void)
      yield* Fiber.interruptAll([buildA, buildB])
    }, Effect.scoped)
  )

  it.effect(
    "fails layer construction when client identity storage cannot be read",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const layerPlatform = Layer.mergeAll(
        Layer.succeed(platform.TabChannel, kit.tabChannel),
        Layer.succeed(platform.WebLocks, kit.webLocks),
        Layer.succeed(platform.EpochStore, kit.epochStore),
        Layer.succeed(platform.ClientIdentityStore, {
          load: () => Effect.die("identity storage unavailable"),
          store: () => Effect.void
        })
      )
      const exit = yield* Effect.exit(
        Layer.build(
          multiTab({
            name: "identity-load-failure",
            definition,
            owner: () => layerOwner,
            requestPersistence: false
          }, layerPlatform).pipe(Layer.provide(Reactivity.layer))
        )
      )
      assert.isTrue(Exit.isFailure(exit))
    }, Effect.scoped)
  )

  it.effect(
    "moves active query retains to the local backend after promotion",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const ownerAReady = yield* Deferred.make<void>()
      const ownerBReady = yield* Deferred.make<void>()
      let ownerBRetains = 0
      const owner = (ready: Deferred.Deferred<void>, onRetain: () => void) => {
        const queryReactivity: QueryReactivity.Service = {
          retain: () => Effect.sync(onRetain).pipe(Effect.as(Effect.void)),
          record: () => Effect.void,
          affected: () => Effect.succeed([])
        }
        return Layer.mergeAll(
          Layer.succeed(Replica.Replica, inertReplica),
          Layer.succeed(EphemeralClient.EphemeralClient, inertEphemeral),
          Layer.effect(
            QueryReactivity.QueryReactivity,
            Deferred.succeed(ready, undefined).pipe(Effect.as(queryReactivity))
          )
        )
      }
      const scopeA = yield* Scope.make()
      yield* Scope.provide(
        Layer.build(
          multiTab({
            name: "retain-promotion",
            definition,
            requestPersistence: false,
            owner: () => owner(ownerAReady, () => {})
          }, kit.layerAll).pipe(Layer.provide(Reactivity.layer))
        ),
        scopeA
      )
      yield* Deferred.await(ownerAReady)
      const scopeB = yield* Scope.make()
      const contextB = yield* Scope.provide(
        Layer.build(
          multiTab({
            name: "retain-promotion",
            definition,
            requestPersistence: false,
            owner: () =>
              owner(ownerBReady, () => {
                ownerBRetains += 1
              })
          }, kit.layerAll).pipe(Layer.provide(Reactivity.layer))
        ),
        scopeB
      )
      const facade = Context.get(contextB, QueryReactivity.QueryReactivity)
      yield* facade.retain("query-key")
      assert.strictEqual(ownerBRetains, 0)
      yield* Scope.close(scopeA, Exit.void)
      yield* Deferred.await(ownerBReady)
      assert.strictEqual(ownerBRetains, 1)
      yield* Scope.close(scopeB, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "closes hosted ephemeral sessions when leadership ends",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const ownerReady = yield* Deferred.make<void>()
      const sessionClosed = yield* Deferred.make<void>()
      const profile = Ephemeral.member({ status: Schema.String })
      const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
      const member = Protocol.EphemeralMember.make({
        clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001"),
        membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
      })
      const hostedEphemeral: EphemeralClient.Service = {
        ...inertEphemeral,
        session: Effect.fnUntraced(function*(memberProfile, options) {
          yield* Effect.addFinalizer(() => Deferred.succeed(sessionClosed, undefined))
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The session is built for the exact erased member profile supplied by the service call.
          return {
            spaceId: options.spaceId,
            member: options.member,
            events: () => Stream.empty,
            state: () => Stream.empty,
            members: Stream.empty,
            updateMember: () => Effect.void
          } as EphemeralClient.Session<typeof memberProfile>
        })
      }
      const defectiveQueryReactivity: QueryReactivity.Service = {
        ...inertQueryReactivity,
        retain: () => Effect.succeed(Effect.die("release failed"))
      }
      const layerHostedOwner = Layer.mergeAll(
        Layer.succeed(Replica.Replica, inertReplica),
        Layer.succeed(EphemeralClient.EphemeralClient, hostedEphemeral),
        Layer.effect(
          QueryReactivity.QueryReactivity,
          Deferred.succeed(ownerReady, undefined).pipe(Effect.as(defectiveQueryReactivity))
        )
      )
      const ownerScope = yield* Scope.make()
      yield* Scope.provide(
        Layer.build(
          multiTab({
            name: "ephemeral-cleanup",
            definition,
            profiles: { member: profile },
            requestPersistence: false,
            owner: () => layerHostedOwner
          }, kit.layerAll).pipe(Layer.provide(Reactivity.layer))
        ),
        ownerScope
      )
      yield* Deferred.await(ownerReady)
      const followerScope = yield* Scope.make()
      const follower = yield* Scope.provide(
        Layer.build(
          multiTab({
            name: "ephemeral-cleanup",
            definition,
            profiles: { member: profile },
            requestPersistence: false,
            owner: () => layerOwner
          }, kit.layerAll).pipe(Layer.provide(Reactivity.layer))
        ),
        followerScope
      )
      const client = Context.get(follower, EphemeralClient.EphemeralClient)
      const reactivity = Context.get(follower, QueryReactivity.QueryReactivity)
      yield* reactivity.retain("query-key")
      yield* Scope.provide(
        client.session(profile, {
          spaceId,
          member,
          value: { status: "online" },
          ttl: 10_000
        }),
        followerScope
      )
      yield* Effect.exit(Scope.close(ownerScope, Exit.void))
      assert.isTrue(yield* Deferred.isDone(sessionClosed))
      yield* Scope.close(followerScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "rejects ephemeral session handles owned by another tab",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const ownerReady = yield* Deferred.make<void>()
      const sessionClosed = yield* Deferred.make<void>()
      const profile = Ephemeral.member({ status: Schema.String })
      const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
      const member = Protocol.EphemeralMember.make({
        clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001"),
        membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
      })
      const hostedEphemeral: EphemeralClient.Service = {
        ...inertEphemeral,
        session: Effect.fnUntraced(function*(memberProfile, options) {
          yield* Effect.addFinalizer(() => Deferred.succeed(sessionClosed, undefined))
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The session is built for the exact erased member profile supplied by the service call.
          return {
            spaceId: options.spaceId,
            member: options.member,
            events: () => Stream.empty,
            state: () => Stream.empty,
            members: Stream.empty,
            updateMember: () => Effect.void
          } as EphemeralClient.Session<typeof memberProfile>
        })
      }
      const layerHostedOwner = Layer.mergeAll(
        Layer.succeed(Replica.Replica, inertReplica),
        Layer.succeed(EphemeralClient.EphemeralClient, hostedEphemeral),
        Layer.effect(
          QueryReactivity.QueryReactivity,
          Deferred.succeed(ownerReady, undefined).pipe(Effect.as(inertQueryReactivity))
        )
      )
      const ownerScope = yield* Scope.make()
      yield* Scope.provide(
        Layer.build(
          multiTab({
            name: "session-owner",
            definition,
            profiles: { member: profile },
            requestPersistence: false,
            owner: () => layerHostedOwner
          }, kit.layerAll).pipe(Layer.provide(Reactivity.layer))
        ),
        ownerScope
      )
      yield* Deferred.await(ownerReady)
      const makeClient = Effect.fnUntraced(function*(tabId: string) {
        const connection = yield* kit.tabChannel.open("@lucas-barake/effect-local-browser:session-owner")
        const protocol = yield* broadcastRpc.makeClientProtocol({
          tabId: Wire.TabId.make(tabId),
          fingerprint: `${Wire.protocolVersion}:${definition.hash}`,
          connection,
          heartbeatInterval: 1000,
          pingInterval: 2000,
          pingTimeout: 4000
        })
        return yield* RpcClient.make(replicaWire.ReplicaRpcs).pipe(
          Effect.provideService(RpcClient.Protocol, protocol)
        )
      })
      const clientA = yield* makeClient("session-a")
      const clientB = yield* makeClient("session-b")
      yield* clientA.Spaces({})
      yield* clientB.Spaces({})
      yield* clientA.EphemeralOpen({
        handle: "shared-handle",
        name: "member",
        spaceId,
        member,
        value: { status: "online" },
        ttlMillis: 10_000
      })
      const closeExit = yield* Effect.exit(clientB.EphemeralClose({ handle: "shared-handle" }))
      assert.isTrue(Exit.isFailure(closeExit))
      assert.isFalse(yield* Deferred.isDone(sessionClosed))
      yield* clientA.EphemeralClose({ handle: "shared-handle" })
      assert.isTrue(yield* Deferred.isDone(sessionClosed))
      yield* Scope.close(ownerScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "preserves SpaceNotJoined when a follower looks up an unknown space",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const ownerReady = yield* Deferred.make<void>()
      const unknown = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000099")
      const rejectingReplica: Replica.Service = {
        ...inertReplica,
        space: (spaceId) => Effect.fail(new ReplicaError.SpaceNotJoined({ spaceId }))
      }
      const layerRejectingOwner = Layer.mergeAll(
        Layer.succeed(Replica.Replica, rejectingReplica),
        Layer.succeed(EphemeralClient.EphemeralClient, inertEphemeral),
        Layer.effect(
          QueryReactivity.QueryReactivity,
          Deferred.succeed(ownerReady, undefined).pipe(Effect.as(inertQueryReactivity))
        )
      )
      const ownerScope = yield* Scope.make()
      yield* Scope.provide(
        Layer.build(
          multiTab({
            name: "space-contract",
            definition,
            requestPersistence: false,
            owner: () => layerRejectingOwner
          }, kit.layerAll).pipe(Layer.provide(Reactivity.layer))
        ),
        ownerScope
      )
      yield* Deferred.await(ownerReady)
      const followerScope = yield* Scope.make()
      const followerContext = yield* Scope.provide(
        Layer.build(
          multiTab({
            name: "space-contract",
            definition,
            requestPersistence: false,
            owner: () => layerOwner
          }, kit.layerAll).pipe(Layer.provide(Reactivity.layer))
        ),
        followerScope
      )
      const facade = Context.get(followerContext, Replica.Replica)
      yield* facade.spaces
      let error: ReplicaError.SpaceNotJoined | undefined
      yield* facade.space(unknown).pipe(
        Effect.asVoid,
        Effect.catchTag("SpaceNotJoined", (failure) =>
          Effect.sync(() => {
            error = failure
          }))
      )
      assert.strictEqual(error?._tag, "SpaceNotJoined")
      assert.strictEqual(error?.spaceId, unknown)
      yield* Scope.close(followerScope, Exit.void)
      yield* Scope.close(ownerScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "reuses a validated follower space handle for later operations",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const ownerReady = yield* Deferred.make<void>()
      const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
      let spaceLookups = 0
      const countingReplica: Replica.Service = {
        ...inertReplica,
        space: (requested) =>
          Effect.sync(() => {
            spaceLookups += 1
            return inertSpace(requested)
          })
      }
      const layerCountingOwner = Layer.mergeAll(
        Layer.succeed(Replica.Replica, countingReplica),
        Layer.succeed(EphemeralClient.EphemeralClient, inertEphemeral),
        Layer.effect(
          QueryReactivity.QueryReactivity,
          Deferred.succeed(ownerReady, undefined).pipe(Effect.as(inertQueryReactivity))
        )
      )
      const ownerScope = yield* Scope.make()
      yield* Scope.provide(
        Layer.build(
          multiTab({
            name: "space-handle",
            definition,
            requestPersistence: false,
            owner: () => layerCountingOwner
          }, kit.layerAll).pipe(Layer.provide(Reactivity.layer))
        ),
        ownerScope
      )
      yield* Deferred.await(ownerReady)
      const followerScope = yield* Scope.make()
      const follower = yield* Scope.provide(
        Layer.build(
          multiTab({
            name: "space-handle",
            definition,
            requestPersistence: false,
            owner: () => layerOwner
          }, kit.layerAll).pipe(Layer.provide(Reactivity.layer))
        ),
        followerScope
      )
      const replica = Context.get(follower, Replica.Replica)
      const space = yield* replica.space(spaceId)
      spaceLookups = 0
      yield* space.status
      assert.strictEqual(spaceLookups, 1)
      yield* Scope.close(followerScope, Exit.void)
      yield* Scope.close(ownerScope, Exit.void)
    }, Effect.scoped)
  )
})
