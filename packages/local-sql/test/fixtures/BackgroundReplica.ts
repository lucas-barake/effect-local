import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Logger from "effect/Logger"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as Statement from "effect/sql/Statement"
import * as Stream from "effect/Stream"
import * as WorkflowEngine from "effect/workflow/WorkflowEngine"
import * as SqlReplica from "../../src/SqlReplica.js"
import * as SyncEngine from "../../src/SyncEngine.js"
import * as Domain from "../Domain.js"
import * as VirtualTime from "./DeterministicTime.js"

export const viewId = Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000801")

export const constructors = ["layer", "layerWorkflow"] as const

export type Constructor = typeof constructors[number]

export type Remote = SyncEngine.SyncEngine["Service"]

export const idleRemote = SyncEngine.SyncEngine.of({
  waitForCredentialChange: () => Effect.never,
  credentialGeneration: Effect.succeed(0),
  transportGeneration: Effect.succeed(0),
  waitForTransportChange: () => Effect.never,
  submitBatch: () => Effect.never,
  discard: () => Effect.die("unexpected discard"),
  pull: () => Effect.never,
  bootstrap: () => Effect.die("unexpected bootstrap"),
  watch: () => Stream.never
})

export const acceptSubmission: Remote["submitBatch"] = (request) =>
  Effect.succeed(Protocol.SubmitBatchResult.make({
    receipts: request.envelopes.map((envelope) =>
      Protocol.AcceptedReceipt.make({
        ...envelope,
        serverSequence: Identity.ServerSequence.make(1),
        result: Domain.todo(envelope.mutationId, "accepted")
      })
    )
  }))

export const emptyPage = (crypto: Crypto.Crypto, request: Parameters<Remote["pull"]>[0]) =>
  Protocol.viewChangesDigest([]).pipe(
    Effect.map((digest) =>
      Protocol.PullPage.make({
        scopeGeneration: request.scopeGeneration,
        cursor: Protocol.ReplicationCursor.make({
          viewId,
          revision: Identity.ReplicationViewRevision.make((request.cursor?.revision ?? 0) + 1)
        }),
        serverSequence: Identity.ServerSequence.make(1),
        changes: [],
        contentBytes: Protocol.encodedBytes([]),
        digest,
        hasMore: false,
        serverSchema: Domain.definition.schemaIdentity
      })
    ),
    Effect.provideService(Crypto.Crypto, crypto)
  )

export const makeAttempts = Effect.gen(function*() {
  const reached = [
    yield* Deferred.make<void>(),
    yield* Deferred.make<void>(),
    yield* Deferred.make<void>(),
    yield* Deferred.make<void>()
  ]
  let count = 0
  const record = Effect.suspend(() => {
    count += 1
    const signal = reached[count - 1]
    if (signal === undefined) return Effect.void
    return Deferred.succeed(signal, undefined).pipe(Effect.asVoid)
  })
  return {
    record,
    count: () => count,
    reached: (attempt: 1 | 2 | 3 | 4) => Deferred.await(reached[attempt - 1])
  }
})

export interface Settings {
  readonly constructor: Constructor
  readonly clientId: Identity.ClientId
  readonly initialSpaces: ReadonlyArray<Identity.SpaceId>
  readonly maximumActiveSpaces: number
  readonly foregroundActiveSpaces: number
  readonly retryDelay: Duration.Input
  readonly maximumRetryDelay: Duration.Input
  readonly reconciliationConcurrency?: number
}

const isStatement = (value: unknown): value is Statement.Statement<unknown> =>
  Statement.isFragment(value) && Effect.isEffect(value)

export const services = Effect.fnUntraced(function*(settings: Settings) {
  const databaseContext = yield* Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    WorkflowEngine.layerMemory
  ).pipe(Layer.build)
  const sql = Context.get(databaseContext, SqlClient.SqlClient)
  let locked: { readonly statement: string; remaining: number } | undefined
  let dying: { readonly statement: string; remaining: number } | undefined
  let heldStatement:
    | {
      readonly statement: string
      readonly pass: boolean
      readonly entered: Deferred.Deferred<void>
      readonly release: Deferred.Deferred<void>
    }
    | undefined
  const failLocked = () => {
    const reason = new SqlError.LockTimeoutError({ cause: "injected lock timeout" })
    return Effect.fail(new SqlError.SqlError({ reason }))
  }
  const lockingSql = new Proxy(sql, {
    apply: (target, thisArg, args: Parameters<typeof sql>) => {
      const source: unknown = args[0]
      if (!Array.isArray(source)) return Reflect.apply(target, thisArg, args)
      const text = source.join("?")
      const held = heldStatement
      if (held !== undefined && text.includes(held.statement)) {
        heldStatement = undefined
        const wait = Deferred.succeed(held.entered, undefined).pipe(Effect.andThen(Deferred.await(held.release)))
        if (!held.pass) return Effect.andThen(wait, Effect.suspend(failLocked))
        const statement = Reflect.apply(target, thisArg, args)
        if (!isStatement(statement)) return statement
        return Effect.andThen(wait, statement)
      }
      if (dying !== undefined && text.includes(dying.statement)) {
        dying.remaining -= 1
        if (dying.remaining <= 0) dying = undefined
        return Effect.die("injected statement defect")
      }
      if (locked === undefined || !text.includes(locked.statement)) return Reflect.apply(target, thisArg, args)
      locked.remaining -= 1
      if (locked.remaining <= 0) locked = undefined
      return failLocked()
    }
  })
  const crypto = Context.get(databaseContext, Crypto.Crypto)
  const reactivity = Context.get(databaseContext, Reactivity.Reactivity)
  let concurrency: { readonly reconciliationConcurrency?: number } = {}
  if (settings.reconciliationConcurrency !== undefined) {
    concurrency = { reconciliationConcurrency: settings.reconciliationConcurrency }
  }
  const options = {
    ...concurrency,
    definition: Domain.definition,
    clientId: settings.clientId,
    initialSpaces: settings.initialSpaces,
    defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    maximumActiveSpaces: settings.maximumActiveSpaces,
    foregroundActiveSpaces: settings.foregroundActiveSpaces,
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 },
    retryDelay: settings.retryDelay,
    maximumRetryDelay: settings.maximumRetryDelay
  } satisfies SqlReplica.Options<typeof Domain.definition>
  let invalidationOutcome:
    | { readonly key: string; readonly outcome: Effect.Effect<void, ReplicaError.ReplicaError> }
    | undefined
  let heldInvalidation:
    | {
      readonly key: string
      readonly matches: (fiberId: number) => Effect.Effect<boolean>
      readonly entered: Deferred.Deferred<void>
      readonly release: Deferred.Deferred<void>
    }
    | undefined
  let heldKeys:
    | {
      readonly keys: ReadonlySet<unknown>
      readonly entered: Set<unknown>
      readonly release: Deferred.Deferred<void>
    }
    | undefined
  const gatedReactivity = new Proxy(reactivity, {
    get: (target, property, receiver) => {
      if (property !== "invalidate") return Reflect.get(target, property, receiver)
      return (keys: Parameters<typeof reactivity.invalidate>[0]) => {
        const holding = heldKeys
        if (holding !== undefined && Array.isArray(keys)) {
          const key: unknown = keys.find((candidate) => holding.keys.has(candidate))
          if (key !== undefined) {
            holding.entered.add(key)
            return target.invalidate(keys).pipe(Effect.andThen(Deferred.await(holding.release)))
          }
        }
        const replaced = invalidationOutcome
        if (replaced !== undefined && Array.isArray(keys) && keys.includes(replaced.key)) return replaced.outcome
        const held = heldInvalidation
        if (held === undefined || !Array.isArray(keys) || !keys.includes(held.key)) {
          return target.invalidate(keys)
        }
        return Effect.withFiber((fiber) => held.matches(fiber.id)).pipe(
          Effect.flatMap((matched) => {
            if (!matched || heldInvalidation !== held) return target.invalidate(keys)
            heldInvalidation = undefined
            return target.invalidate(keys).pipe(
              Effect.andThen(Deferred.succeed(held.entered, undefined)),
              Effect.andThen(Deferred.await(held.release))
            )
          })
        )
      }
    }
  })
  const engine = Context.get(databaseContext, WorkflowEngine.WorkflowEngine)
  const executions = new Map<string, Set<string>>()
  const countingEngine = new Proxy(engine, {
    get: (target, property, receiver) => {
      if (property !== "execute") return Reflect.get(target, property, receiver)
      return (...args: ReadonlyArray<unknown>): unknown => {
        const [workflow, execution] = args
        if (Predicate.hasProperty(workflow, "_tag") && Predicate.hasProperty(execution, "executionId")) {
          const tag = String(workflow._tag)
          const started = executions.get(tag) ?? new Set<string>()
          started.add(String(execution.executionId))
          executions.set(tag, started)
        }
        return Reflect.apply(target.execute, target, args)
      }
    }
  })
  const lockingContext = databaseContext.pipe(
    Context.add(SqlClient.SqlClient, lockingSql),
    Context.add(Reactivity.Reactivity, gatedReactivity),
    Context.add(WorkflowEngine.WorkflowEngine, countingEngine)
  )
  const start = (remote: Remote) => {
    const layerServices = Layer.mergeAll(
      Domain.layerHandlers,
      Layer.succeed(SyncEngine.SyncEngine, remote),
      Layer.succeedContext(lockingContext)
    )
    let layerReplica = SqlReplica.layer(options).pipe(Layer.provide(layerServices))
    if (settings.constructor === "layerWorkflow") {
      layerReplica = SqlReplica.layerWorkflow(options).pipe(Layer.provide(layerServices))
    }
    return Layer.build(layerReplica).pipe(Effect.map(Context.get(Replica.Replica)))
  }
  const lockNext = (statement: string, times = 1) => {
    locked = { statement, remaining: times }
  }
  const dieNext = (statement: string, times = 1) => {
    dying = { statement, remaining: times }
  }
  const holdStatement = Effect.fnUntraced(function*(statement: string, pass = false) {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    heldStatement = { statement, pass, entered, release }
    return { entered: Deferred.await(entered), release: Deferred.succeed(release, undefined) }
  })
  const holdInvalidation = Effect.fnUntraced(function*(key: string) {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const arm = (remaining: number) => {
      let left = remaining
      const matches = () =>
        Effect.sync(() => {
          left -= 1
          return left <= 0
        })
      heldInvalidation = { key, matches, entered, release }
    }
    return { arm, entered: Deferred.await(entered), release: Deferred.succeed(release, undefined) }
  })
  const holdInvalidationsOf = Effect.fnUntraced(function*(keys: ReadonlyArray<string>) {
    const release = yield* Deferred.make<void>()
    const entered = new Set<unknown>()
    heldKeys = { keys: new Set(keys), entered, release }
    return { entered: () => entered.size, release: Deferred.succeed(release, undefined) }
  })
  const holdInvalidationWhen = Effect.fnUntraced(function*(
    key: string,
    matches: (fiberId: number) => Effect.Effect<boolean>
  ) {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    heldInvalidation = { key, matches, entered, release }
    return { entered: Deferred.await(entered), release: Deferred.succeed(release, undefined) }
  })
  const endInvalidationsWith = (key: string, outcome: Effect.Effect<void, ReplicaError.ReplicaError>) => {
    invalidationOutcome = { key, outcome }
  }
  const lockRemaining = () => locked?.remaining ?? 0
  const workflowExecutions = (spaceId: Identity.SpaceId) => {
    let started = 0
    for (const [name, ids] of executions) {
      if (name.includes(spaceId)) started += ids.size
    }
    return started
  }
  return {
    sql,
    crypto,
    reactivity,
    start,
    lockNext,
    lockRemaining,
    dieNext,
    holdStatement,
    holdInvalidation,
    holdInvalidationWhen,
    holdInvalidationsOf,
    endInvalidationsWith,
    workflowExecutions
  }
})

export type Services = Effect.Success<ReturnType<typeof services>>

export const seedPending = Effect.fnUntraced(function*(
  background: Services,
  spaceIds: ReadonlyArray<Identity.SpaceId>
) {
  const seedScope = yield* Scope.make()
  const seedReplica = yield* background.start(idleRemote).pipe(Scope.provide(seedScope))
  for (const spaceId of spaceIds) {
    const seedSpace = yield* seedReplica.space(spaceId)
    yield* seedSpace.mutate(Domain.PutTodo, Domain.todo("pending"))
    yield* seedSpace.deactivate
  }
  yield* Scope.close(seedScope, Exit.void)
  yield* background.sql`UPDATE effect_local_client_spaces
    SET replication_view_id = ${viewId}, replication_view_revision = 0`
})

export const awaitSpaceStatusWhere = Effect.fnUntraced(function*(
  space: Replica.Space,
  reactivity: Reactivity.Reactivity,
  matches: (status: ReplicaStatus.SpaceStatus) => boolean
) {
  const changes = yield* Queue.unbounded<void>()
  yield* Effect.acquireRelease(
    Effect.sync(() =>
      reactivity.registerUnsafe([ReactivityKey.status(space.spaceId), ReactivityKey.aggregateStatus], () => {
        Queue.offerUnsafe(changes, undefined)
      })
    ),
    (unregister) => Effect.sync(unregister)
  )
  let status = yield* space.status
  while (!matches(status)) {
    yield* Queue.take(changes)
    status = yield* space.status
  }
  return status
})

export const awaitSpaceStatus = (
  space: Replica.Space,
  reactivity: Reactivity.Reactivity,
  tag: ReplicaStatus.ReplicaStatus["_tag"]
) => awaitSpaceStatusWhere(space, reactivity, (status) => status._tag === tag)

export const eventually = (
  background: Services,
  space: Replica.Space,
  matches: (status: ReplicaStatus.SpaceStatus) => boolean
) =>
  awaitSpaceStatusWhere(space, background.reactivity, matches).pipe(
    Effect.scoped,
    VirtualTime.advanceUntil,
    Effect.timeoutOption("5 minutes")
  )

export const within = <A, E extends { readonly _tag: string },>(effect: Effect.Effect<A, E>) =>
  effect.pipe(Effect.exit, Effect.timeoutOption("5 minutes"), VirtualTime.advanceUntil)

export const makeCapacityProbe = Effect.fnUntraced(function*(spaceIds: ReadonlyArray<Identity.SpaceId>) {
  const release = yield* Deferred.make<void>()
  let armed = false
  let inFlight = 0
  const held = <A, E extends { readonly _tag: string },>(spaceId: Identity.SpaceId, answer: Effect.Effect<A, E>) =>
    Effect.suspend(() => {
      if (!armed || !spaceIds.includes(spaceId)) return answer
      inFlight += 1
      return Deferred.await(release).pipe(
        Effect.ensuring(Effect.sync(() => {
          inFlight -= 1
        })),
        Effect.andThen(answer)
      )
    })
  const fill = Effect.fnUntraced(
    function*(background: Services, replica: Replica.Replica["Service"], home: Identity.SpaceId) {
      armed = true
      for (const spaceId of spaceIds) {
        const space = yield* replica.space(spaceId)
        yield* space.mutate(Domain.PutTodo, Domain.todo("probe")).pipe(VirtualTime.advanceUntil)
      }
      const current = yield* replica.space(home)
      yield* current.mutate(Domain.PutTodo, Domain.todo("probe")).pipe(VirtualTime.advanceUntil)
      const foregroundSynced = yield* eventually(background, current, isOnlineDrained)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))
      const backgroundCallsAtOnce = inFlight
      yield* Deferred.succeed(release, undefined)
      let drained = 0
      for (const spaceId of spaceIds) {
        const space = yield* replica.space(spaceId)
        if (Option.isSome(yield* eventually(background, space, (status) => status.pending === 0))) drained += 1
      }
      return { foregroundSynced: Option.isSome(foregroundSynced), backgroundCallsAtOnce, drained }
    }
  )
  return { held, fill }
})

export const isOnlineDrained = (status: ReplicaStatus.SpaceStatus) => status._tag === "Online" && status.pending === 0

export const healthyRemote = (background: Services) =>
  SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: acceptSubmission,
    pull: (request) => emptyPage(background.crypto, request)
  })

export const installView = (background: Services) =>
  background.sql`UPDATE effect_local_client_spaces SET replication_view_id = ${viewId}, replication_view_revision = 0`

export const count = (background: Services, key: string) => {
  let delivered = 0
  background.reactivity.registerUnsafe([key], () => {
    delivered += 1
  })
  return () => delivered
}

export const captureErrors = () => {
  const messages: Array<string> = []
  const logger = Logger.make<unknown, void>((entry) => {
    if (entry.logLevel !== "Error") return
    let message: unknown = entry.message
    if (Array.isArray(message)) message = message[0]
    messages.push(String(message))
  })
  return { layerLogs: Logger.layer([logger]), messages: () => messages }
}

export const describeExit = <A, E extends { readonly _tag: string },>(exit: Option.Option<Exit.Exit<A, E>>) => {
  if (Option.isNone(exit)) return "never completed"
  if (Exit.isSuccess(exit.value)) return "succeeded"
  if (Cause.hasInterrupts(exit.value.cause)) return "interrupted"
  if (Cause.hasDies(exit.value.cause)) return "died"
  return "failed"
}
