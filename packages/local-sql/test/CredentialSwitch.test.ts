import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Stream from "effect/Stream"
import * as WorkflowEngine from "effect/workflow/WorkflowEngine"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import { awaitSpaceStatusWhere, constructors, describeExit, within } from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const first = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f501")
const second = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f502")
const ownClient = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000f501")
const otherClient = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000f502")
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })

const layerServerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)

const layerServer = ServerStore.layerTrusted({ definition: Domain.definition, migration }).pipe(
  Layer.provide(MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))),
  Layer.provide(layerServerDatabase)
)

interface Hold {
  readonly entered: Deferred.Deferred<void>
  readonly release: Deferred.Deferred<void>
  skip: number
}

const makeHold = Effect.fnUntraced(function*(skip = 0) {
  const hold: Hold = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>(), skip }
  return hold
})

const client = Effect.fnUntraced(function*(
  server: ServerStore.Service,
  constructor: typeof constructors[number],
  clientId: Identity.ClientId,
  watching: boolean
) {
  const database = yield* Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    WorkflowEngine.layerMemory
  ).pipe(Layer.build)
  const reactivity = Context.get(database, Reactivity.Reactivity)
  const controls: {
    principal: "P" | "Q"
    generation: number
    pulls: Hold | undefined
    submits: Hold | undefined
    bootstraps: Hold | undefined
    bootstrapped: number
    denials: Deferred.Deferred<void> | undefined
  } = {
    principal: "P",
    generation: 0,
    pulls: undefined,
    submits: undefined,
    bootstraps: undefined,
    bootstrapped: 0,
    denials: undefined
  }
  const held = (hold: Hold | undefined) => {
    if (hold === undefined) return Effect.void
    if (hold.skip > 0) {
      hold.skip -= 1
      return Effect.void
    }
    return Deferred.succeed(hold.entered, undefined).pipe(Effect.andThen(Deferred.await(hold.release)))
  }
  const authorized = <A,>(call: Effect.Effect<A, ReplicaError.ReplicaError>) =>
    Effect.suspend(() => {
      if (controls.principal === "P") return call
      const denied = Effect.fail(new ReplicaError.AuthorizationDenied({ reason: null }))
      if (controls.denials === undefined) return denied
      return Effect.andThen(Deferred.await(controls.denials), denied)
    })
  const remote = SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    credentialGeneration: Effect.sync(() => controls.generation),
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) =>
      authorized(Effect.suspend(() => {
        let hold: Hold | undefined
        if (request.envelopes[0].spaceId === first) hold = controls.submits
        return Effect.tap(server.admitBatch(request, null), () => held(hold))
      })),
    discard: (request) => server.discard(request, null),
    pull: (request) =>
      authorized(Effect.suspend(() => {
        let hold: Hold | undefined
        if (request.spaceId === first) {
          hold = controls.pulls
        }
        return Effect.tap(server.pull(request), () => held(hold))
      })),
    bootstrap: (request) =>
      authorized(Effect.suspend(() => {
        controls.bootstrapped += 1
        return Effect.tap(server.bootstrap(request), () => held(controls.bootstraps))
      })),
    watch: (request) => {
      if (!watching) return Stream.never
      return server.watch(request)
    }
  })
  const options = {
    definition: Domain.definition,
    clientId,
    initialSpaces: [first, second],
    defaultScope: scope,
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 1,
    pageSize: 2,
    migration,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  } as const
  const layerServices = Layer.mergeAll(
    Domain.layerHandlers,
    Layer.succeed(SyncEngine.SyncEngine, remote),
    Layer.succeedContext(database)
  )
  let layerReplica = SqlReplica.layer(options).pipe(Layer.provide(layerServices))
  if (constructor === "layerWorkflow") {
    layerReplica = SqlReplica.layerWorkflow(options).pipe(Layer.provide(layerServices))
  }
  const replica = Context.get(yield* Layer.build(layerReplica), Replica.Replica)
  const a = yield* replica.space(first)
  const b = yield* replica.space(second)
  const until = (space: Replica.Space, matches: (status: ReplicaStatus.SpaceStatus) => boolean) =>
    awaitSpaceStatusWhere(space, reactivity, matches).pipe(
      Effect.scoped,
      VirtualTime.advanceUntil,
      Effect.timeoutOption("10 minutes")
    )
  const drained = (status: ReplicaStatus.SpaceStatus) => status._tag === "Online" && status.pending === 0
  const read = (id: string) =>
    VirtualTime.advanceUntil(a.get(Domain.Todo, id)).pipe(
      Effect.map(Option.map((todo) => todo.title)),
      Effect.map(Option.getOrNull)
    )
  const applied = (id: string) => {
    let count = 0
    reactivity.registerUnsafe([ReactivityKey.entity(first, Domain.Todo.name, id)], () => {
      count += 1
    })
    return () => count
  }
  const signInAsSecond = () => {
    controls.principal = "Q"
    controls.generation += 1
  }
  return { a, b, controls, until, drained, read, applied, signInAsSecond }
})

const quiet = (duration: "1 millis" | "500 millis" | "1 minute" | "10 minutes") =>
  VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption(duration))

const world = Effect.fnUntraced(function*(constructor: typeof constructors[number]) {
  const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const own = yield* client(server, constructor, ownClient, false)
  const other = yield* client(server, "layer", otherClient, true)
  yield* VirtualTime.advanceUntil(own.a.activate)
  assert.isTrue(Option.isSome(yield* own.until(own.a, own.drained)), "the client under test came online")
  yield* VirtualTime.advanceUntil(other.a.activate)
  assert.isTrue(Option.isSome(yield* other.until(other.a, other.drained)), "the other client came online")
  const otherWrites = Effect.fnUntraced(function*(ids: ReadonlyArray<string>) {
    for (const id of ids) {
      yield* other.a.mutate(Domain.PutTodo, Domain.todo(id, `other-${id}`)).pipe(VirtualTime.advanceUntil)
    }
    assert.isTrue(
      Option.isSome(yield* other.until(other.a, other.drained)),
      "the other client's writes reached the server"
    )
  })
  const evict = Effect.gen(function*() {
    const evicted = yield* within(own.b.get(Domain.Todo, "elsewhere"))
    assert.strictEqual(describeExit(evicted), "succeeded", "the second space took the only foreground place")
  })
  return { own, other, otherWrites, evict }
})

const credentialRows = constructors.flatMap((constructor) => ["evicted", "live"].map((turn) => ({ constructor, turn })))

describe("a server answer obtained for one principal after another signed in", () => {
  it.effect.each(credentialRows)(
    "does not apply a pulled page ($constructor, $turn turn)",
    Effect.fnUntraced(function*(row) {
      const { evict, otherWrites, own } = yield* world(row.constructor)
      yield* otherWrites(["secret"])
      const applied = own.applied("secret")
      const hold = yield* makeHold()
      own.controls.pulls = hold
      own.controls.denials = yield* Deferred.make<void>()
      yield* own.a.mutate(Domain.PutTodo, Domain.todo("mine", "mine")).pipe(VirtualTime.advanceUntil)
      yield* VirtualTime.advanceUntil(Deferred.await(hold.entered))

      own.signInAsSecond()
      if (row.turn === "evicted") yield* evict
      yield* quiet("1 millis")
      yield* Deferred.succeed(hold.release, undefined)
      yield* quiet("500 millis")
      const appliedForSecond = applied()
      yield* Deferred.succeed(own.controls.denials, undefined)
      yield* quiet("1 minute")

      assert.strictEqual(appliedForSecond, 0, "the first principal's row reached the local store")
      assert.strictEqual(yield* own.read("secret"), null)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not apply the receipts of a submit with %s",
    Effect.fnUntraced(function*(constructor) {
      const { own } = yield* world(constructor)
      const hold = yield* makeHold()
      own.controls.submits = hold
      own.controls.denials = yield* Deferred.make<void>()
      const committed = yield* own.a.mutate(Domain.PutTodo, Domain.todo("mine", "mine")).pipe(VirtualTime.advanceUntil)
      yield* VirtualTime.advanceUntil(Deferred.await(hold.entered))

      own.signInAsSecond()
      yield* Deferred.succeed(hold.release, undefined)
      yield* quiet("500 millis")
      const receipt = yield* own.a.receipt(Domain.PutTodo, committed.envelope.mutationId).pipe(VirtualTime.advanceUntil)

      assert.isTrue(Option.isNone(receipt), "the receipt obtained for the first principal was not stored")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not install a bootstrap page with %s",
    Effect.fnUntraced(function*(constructor) {
      const { other, own } = yield* world(constructor)
      yield* other.b.mutate(Domain.PutTodo, Domain.todo("secret", "other-secret")).pipe(VirtualTime.advanceUntil)
      yield* other.until(other.b, other.drained)
      const hold = yield* makeHold()
      own.controls.bootstraps = hold
      own.controls.denials = yield* Deferred.make<void>()
      const before = own.controls.bootstrapped
      yield* Effect.forkChild(own.b.activate, { startImmediately: true })
      yield* VirtualTime.advanceUntil(Deferred.await(hold.entered))

      own.signInAsSecond()
      yield* Deferred.succeed(hold.release, undefined)
      yield* quiet("500 millis")
      const status = yield* own.b.status

      assert.isAbove(own.controls.bootstrapped, before, "the space was bootstrapping")
      assert.isFalse(status.synced, "the snapshot obtained for the first principal was not installed")
    }, VirtualTime.scoped)
  )
})
