import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Activity from "effect/unstable/workflow/Activity"
import * as DurableClock from "effect/unstable/workflow/DurableClock"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as Configuration from "./internal/configuration.js"
import * as LocalStore from "./LocalStore.js"
import * as Reconciler from "./Reconciler.js"
import * as SyncEngine from "./SyncEngine.js"

export const ReconciliationGeneration = pipe(
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
  (maximumSafeInteger) => Schema.Int.check(Schema.isGreaterThan(0), maximumSafeInteger)
)

const PayloadFields = {
  schemaIdentity: Schema.String,
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  membershipIncarnation: Identity.MembershipIncarnation,
  scope: Protocol.ReplicationScope,
  scopeGeneration: Identity.ReplicationScopeGeneration,
  generation: ReconciliationGeneration
} as const

export const Payload = Schema.Struct(PayloadFields)
export type Payload = typeof Payload.Type

const schemaIdentityKey = (definition: Definition.Any): string =>
  `${definition.schemaIdentity.version}:${definition.schemaIdentity.hash}`

type ReplicaIdentity = Pick<Payload, "schemaIdentity" | "spaceId" | "clientId" | "membershipIncarnation">

interface RegisteredSchema {
  readonly version: number
  readonly hash: string
}

interface WorkflowRegistrationState {
  readonly schemas: Map<string, RegisteredSchema>
}

const workflowRegistrationStateSymbol = Symbol.for(
  "@lucas-barake/effect-local-sql/ReconciliationWorkflow/WorkflowRegistrationState"
)

const Json = Schema.fromJsonString(Schema.Unknown)
const encodeJson = Schema.encodeEffect(Json)

type EngineWithRegistrationState = WorkflowEngine.WorkflowEngine["Service"] & {
  [workflowRegistrationStateSymbol]?: WorkflowRegistrationState
}

const replicaIdentityKey = (options: {
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly membershipIncarnation: Identity.MembershipIncarnation
}) =>
  encodeJson([options.spaceId, options.clientId, options.membershipIncarnation]).pipe(
    Effect.catchTag("SchemaError", (error) => Effect.die(error))
  )

const workflowRegistrationState = (
  engine: WorkflowEngine.WorkflowEngine["Service"]
): WorkflowRegistrationState => {
  const owned: EngineWithRegistrationState = engine
  if (owned[workflowRegistrationStateSymbol] !== undefined) return owned[workflowRegistrationStateSymbol]
  const state: WorkflowRegistrationState = { schemas: new Map() }
  Object.defineProperty(owned, workflowRegistrationStateSymbol, { value: state })
  return state
}

export const make = ({
  clientId,
  membershipIncarnation,
  schemaIdentity: workflowSchemaIdentity,
  spaceId
}: ReplicaIdentity) => {
  const workflowIdentityValues = [workflowSchemaIdentity, spaceId, clientId, membershipIncarnation]
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Workflow.make requires its durable tag as a synchronous string.
  const encodeWorkflowIdentity = Schema.encodeSync(Json)
  const workflowIdentity = encodeWorkflowIdentity(workflowIdentityValues)
  return Workflow.make(
    `effect-local/ReconcileReplica/v4/${workflowIdentity}`,
    {
      payload: PayloadFields,
      success: Schema.Void,
      error: ReplicaError.ReplicaError,
      idempotencyKey: (payload) => {
        const key = [
          payload.schemaIdentity,
          payload.spaceId,
          payload.clientId,
          payload.membershipIncarnation,
          payload.scope,
          payload.scopeGeneration,
          payload.generation
        ]
        // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Workflow.idempotencyKey must synchronously return a string.
        const encodeKey = Schema.encodeSync(Json)
        return encodeKey(key)
      }
    }
  )
}

export const Execution = Schema.Struct({
  ...PayloadFields,
  executionId: Schema.String
})
export type Execution = typeof Execution.Type

export interface RegistrationService {
  readonly registered: true
}

export class Registration extends Context.Service<Registration, RegistrationService>()(
  "@lucas-barake/effect-local-sql/ReconciliationWorkflow/Registration"
) {}

export class RegistrationScope extends Context.Service<RegistrationScope, Scope.Scope>()(
  "@lucas-barake/effect-local-sql/ReconciliationWorkflow/RegistrationScope"
) {}

export interface RuntimeServices {
  readonly local: LocalStore.Service
  readonly reconciliation: Reconciler.ReconciliationService
}

export interface RuntimeLeaseService {
  readonly acquire: Effect.Effect<RuntimeServices, ReplicaError.ReplicaError, Scope.Scope>
  readonly admit: <A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E, R>
}

export class RuntimeLease extends Context.Service<RuntimeLease, RuntimeLeaseService>()(
  "@lucas-barake/effect-local-sql/ReconciliationWorkflow/RuntimeLease"
) {}

export const executionId = (payload: Payload) => make(payload).executionId(payload)

export const start = Effect.fnUntraced(function*(
  payload: Payload
): Effect.fn.Return<Execution, never, WorkflowEngine.WorkflowEngine> {
  const workflowExecutionId = yield* make(payload).execute(payload, { discard: true })
  return Execution.make({ ...payload, executionId: workflowExecutionId })
})

export const validateExecution = (execution: Execution): Effect.Effect<Payload, ReplicaError.ProtocolInvalid> =>
  make(execution).executionId(execution).pipe(
    Effect.flatMap((expected) => {
      if (expected === execution.executionId) return Effect.succeed(Payload.make(execution))
      return Effect.fail(
        new ReplicaError.ProtocolInvalid({ message: "Reconciliation execution ID does not match payload" })
      )
    })
  )

export const poll = (execution: Execution) =>
  validateExecution(execution).pipe(Effect.andThen(make(execution).poll(execution.executionId)))

export const interrupt = (execution: Execution) =>
  validateExecution(execution).pipe(Effect.andThen(make(execution).interrupt(execution.executionId)))

export const resume = (execution: Execution) =>
  validateExecution(execution).pipe(Effect.andThen(make(execution).resume(execution.executionId)))

export interface Options {
  readonly definition: Definition.Any
  readonly evolution?: Evolution.Evolution
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly retryDelay?: Duration.Input
  readonly maximumRetryDelay?: Duration.Input
  readonly maximumAttempts?: number
}

interface RetryConfigurationService {
  readonly retryDelayMillis: number
  readonly maximumRetryDelayMillis: number
  readonly maximumAttempts: number
}

class RetryConfiguration extends Context.Service<RetryConfiguration, RetryConfigurationService>()(
  "@lucas-barake/effect-local-sql/ReconciliationWorkflow/RetryConfiguration"
) {}

const layerRetryConfiguration = (options: Options) =>
  Layer.effect(
    RetryConfiguration,
    Effect.gen(function*() {
      const maximumAttempts = options.maximumAttempts ?? 8
      if (!Number.isSafeInteger(maximumAttempts) || maximumAttempts <= 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "maximumAttempts",
          message: "maximumAttempts must be a positive safe integer"
        })
      }
      const retryTiming = yield* Configuration.retryTiming(options)
      return RetryConfiguration.of({
        ...retryTiming,
        maximumAttempts
      })
    })
  )

const handler = (
  options: Options,
  configuration: RetryConfigurationService,
  registrationState: WorkflowRegistrationState,
  membershipIncarnation: Identity.MembershipIncarnation,
  lease: RuntimeLeaseService
) =>
  Effect.fnUntraced(function*(payload: Payload) {
    if (
      payload.schemaIdentity !== schemaIdentityKey(options.definition) ||
      payload.spaceId !== options.spaceId ||
      payload.clientId !== options.clientId
    ) {
      return yield* new ReplicaError.ProtocolInvalid({
        message: "Reconciliation workflow payload does not match this replica"
      })
    }
    if (payload.membershipIncarnation !== membershipIncarnation) {
      return yield* new ReplicaError.SpaceUnavailable({ spaceId: payload.spaceId })
    }
    const registeredKey = yield* replicaIdentityKey({
      ...options,
      membershipIncarnation
    })
    const registered = registrationState.schemas.get(registeredKey)
    if (
      registered !== undefined &&
      (registered.version !== options.definition.schemaIdentity.version ||
        registered.hash !== options.definition.schemaIdentity.hash)
    ) {
      return yield* new ReplicaError.StaleSchema({
        expectedVersion: registered.version,
        expectedHash: registered.hash,
        actualVersion: options.definition.schemaIdentity.version,
        actualHash: options.definition.schemaIdentity.hash
      })
    }
    const validateScope = Effect.fnUntraced(function*(local: LocalStore.Service) {
      const state = yield* local.replicationState
      if (state.scopeGeneration !== payload.scopeGeneration) {
        return yield* new ReplicaError.StaleReplicationScope({
          expected: state.scopeGeneration,
          actual: payload.scopeGeneration
        })
      }
      if (
        state.scope.models.length !== payload.scope.models.length ||
        state.scope.models.some((model, index) => model !== payload.scope.models[index])
      ) {
        return yield* new ReplicaError.ProtocolInvalid({
          message: "Reconciliation workflow scope does not match its durable generation"
        })
      }
      return yield* Effect.void
    })
    const runActivity = Effect.fnUntraced(function*(
      name: string,
      execute: (runtime: RuntimeServices) => Effect.Effect<void, ReplicaError.ReplicaError>
    ) {
      let attempt = 1
      while (true) {
        const result = yield* Activity.make({
          name: `${name}/${attempt}`,
          error: ReplicaError.ReplicaError,
          execute: Effect.scoped(lease.acquire.pipe(
            Effect.flatMap((runtime) => {
              if (runtime.local.membershipIncarnation !== membershipIncarnation) {
                return Effect.fail(new ReplicaError.SpaceUnavailable({ spaceId: payload.spaceId }))
              }
              return lease.admit(execute(runtime))
            })
          ))
        }).pipe(Effect.result)
        if (Result.isSuccess(result)) return
        yield* Effect.scoped(lease.acquire.pipe(
          Effect.flatMap((runtime) => runtime.reconciliation.failed(result.failure))
        ))
        if (
          result.failure._tag === "CredentialRejected" ||
          result.failure._tag === "ProtocolInvalid" ||
          result.failure._tag === "StaleSchema" ||
          result.failure._tag === "SpaceUnavailable" ||
          result.failure._tag === "StaleReplicationScope" ||
          result.failure._tag === "UpgradeRequired" ||
          result.failure._tag === "AuthorizationDenied" ||
          attempt >= configuration.maximumAttempts
        ) {
          yield* result.failure
          return
        }
        yield* DurableClock.sleep({
          name: `${name}-retry/${attempt}`,
          duration: Configuration.retryMillis(configuration, attempt),
          inMemoryThreshold: Duration.zero
        })
        attempt += 1
      }
    })

    yield* runActivity("sync", ({ local, reconciliation }) =>
      validateScope(local).pipe(
        Effect.andThen(reconciliation.sync),
        Effect.andThen(validateScope(local))
      ))
    yield* runActivity("complete", ({ local }) =>
      Effect.andThen(validateScope(local), local.completeReconciliation(payload.generation)))
    yield* Effect.scoped(lease.acquire.pipe(
      Effect.flatMap((runtime) =>
        runtime.reconciliation.succeeded
      )
    ))
    return undefined
  })

const layerRegistrationWithConfiguration = (
  options: Options
): Layer.Layer<
  Registration,
  ReplicaError.InvalidConfiguration,
  LocalStore.Store | Reconciler.Reconciliation | RetryConfiguration | WorkflowEngine.WorkflowEngine
> =>
  Layer.unwrap(Effect.gen(function*() {
    const configuration = yield* RetryConfiguration
    const engine = yield* WorkflowEngine.WorkflowEngine
    const local = yield* LocalStore.Store
    const reconciliation = yield* Reconciler.Reconciliation
    const currentScope = yield* Effect.scope
    const registrationScope = Option.getOrElse(
      yield* Effect.serviceOption(RegistrationScope),
      () => currentScope
    )
    const evolution = options.evolution ?? Evolution.make({ current: options.definition })
    const registrationState = workflowRegistrationState(engine)
    const identity = {
      ...options,
      membershipIncarnation: local.membershipIncarnation
    }
    const replicaKey = yield* replicaIdentityKey(identity)
    const workflow = make({
      schemaIdentity: schemaIdentityKey(options.definition),
      spaceId: options.spaceId,
      clientId: options.clientId,
      membershipIncarnation: local.membershipIncarnation
    })
    const lease = RuntimeLease.of({
      acquire: Effect.acquireRelease(
        Effect.succeed({ local, reconciliation }),
        () => Effect.void
      ),
      admit: (effect) => effect
    })
    const workflowHandler = handler(
      options,
      configuration,
      registrationState,
      local.membershipIncarnation,
      lease
    )
    yield* engine.register(workflow, workflowHandler).pipe(Scope.provide(registrationScope))
    const registered = registrationState.schemas.get(replicaKey)
    if (
      registered !== undefined &&
      registered.version === options.definition.schemaIdentity.version &&
      registered.hash !== options.definition.schemaIdentity.hash
    ) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "definition",
        message: `Reconciliation workflow schema version ${registered.version} is already registered with another hash`
      })
    }
    if (registered === undefined || registered.version < options.definition.schemaIdentity.version) {
      registrationState.schemas.set(replicaKey, options.definition.schemaIdentity)
    }
    const legacySchemas = new Map<string, Definition.Any>()
    for (const step of evolution.steps) {
      legacySchemas.set(schemaIdentityKey(step.from), step.from)
    }
    for (const baseline of evolution.legacyBaselines) {
      legacySchemas.set(schemaIdentityKey(baseline.definition), baseline.definition)
    }
    legacySchemas.delete(schemaIdentityKey(options.definition))
    for (const [legacyIdentity, definition] of legacySchemas) {
      yield* engine.register(
        make({
          schemaIdentity: legacyIdentity,
          spaceId: options.spaceId,
          clientId: options.clientId,
          membershipIncarnation: local.membershipIncarnation
        }),
        Effect.fnUntraced(function*() {
          const current = registrationState.schemas.get(replicaKey) ?? options.definition.schemaIdentity
          return yield* new ReplicaError.StaleSchema({
            expectedVersion: current.version,
            expectedHash: current.hash,
            actualVersion: definition.schemaIdentity.version,
            actualHash: definition.schemaIdentity.hash
          })
        })
      ).pipe(Scope.provide(registrationScope))
    }
    return pipe(Registration.of({ registered: true }), Layer.succeed(Registration))
  }))

export const layerRegistration = (options: Options) => {
  return layerRegistrationWithConfiguration(options).pipe(Layer.provide(layerRetryConfiguration(options)))
}

export interface DetachedRegistrationOptions extends Options {
  readonly membershipIncarnation: Identity.MembershipIncarnation
}

const layerDetachedRegistrationWithConfiguration = (
  options: DetachedRegistrationOptions
): Layer.Layer<
  Registration,
  ReplicaError.InvalidConfiguration,
  RuntimeLease | RetryConfiguration | WorkflowEngine.WorkflowEngine
> =>
  Layer.unwrap(Effect.gen(function*() {
    const configuration = yield* RetryConfiguration
    const engine = yield* WorkflowEngine.WorkflowEngine
    const lease = yield* RuntimeLease
    const currentScope = yield* Effect.scope
    const registrationScope = Option.getOrElse(
      yield* Effect.serviceOption(RegistrationScope),
      () => currentScope
    )
    const evolution = options.evolution ?? Evolution.make({ current: options.definition })
    const registrationState = workflowRegistrationState(engine)
    const replicaKey = yield* replicaIdentityKey(options)
    const workflow = make({
      schemaIdentity: schemaIdentityKey(options.definition),
      spaceId: options.spaceId,
      clientId: options.clientId,
      membershipIncarnation: options.membershipIncarnation
    })
    yield* engine.register(
      workflow,
      handler(
        options,
        configuration,
        registrationState,
        options.membershipIncarnation,
        lease
      )
    ).pipe(Scope.provide(registrationScope))
    const registered = registrationState.schemas.get(replicaKey)
    if (
      registered !== undefined &&
      registered.version === options.definition.schemaIdentity.version &&
      registered.hash !== options.definition.schemaIdentity.hash
    ) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "definition",
        message: `Reconciliation workflow schema version ${registered.version} is already registered with another hash`
      })
    }
    if (registered === undefined || registered.version < options.definition.schemaIdentity.version) {
      registrationState.schemas.set(replicaKey, options.definition.schemaIdentity)
    }
    const legacySchemas = new Map<string, Definition.Any>()
    for (const step of evolution.steps) legacySchemas.set(schemaIdentityKey(step.from), step.from)
    for (const baseline of evolution.legacyBaselines) {
      legacySchemas.set(schemaIdentityKey(baseline.definition), baseline.definition)
    }
    legacySchemas.delete(schemaIdentityKey(options.definition))
    for (const [legacyIdentity, definition] of legacySchemas) {
      yield* engine.register(
        make({
          schemaIdentity: legacyIdentity,
          spaceId: options.spaceId,
          clientId: options.clientId,
          membershipIncarnation: options.membershipIncarnation
        }),
        Effect.fnUntraced(function*() {
          const current = registrationState.schemas.get(replicaKey) ?? options.definition.schemaIdentity
          return yield* new ReplicaError.StaleSchema({
            expectedVersion: current.version,
            expectedHash: current.hash,
            actualVersion: definition.schemaIdentity.version,
            actualHash: definition.schemaIdentity.hash
          })
        })
      ).pipe(Scope.provide(registrationScope))
    }
    return Layer.succeed(Registration, { registered: true })
  }))

export const layerDetachedRegistration = (options: DetachedRegistrationOptions) =>
  layerDetachedRegistrationWithConfiguration(options).pipe(
    Layer.provide(layerRetryConfiguration(options))
  )

const layerSchedulerWithConfiguration = (
  options: Options
): Layer.Layer<
  Reconciler.Reconciler,
  ReplicaError.ReplicaError,
  | LocalStore.Store
  | Reconciler.Reconciliation
  | Registration
  | RetryConfiguration
  | SyncEngine.SyncEngine
  | WorkflowEngine.WorkflowEngine
> =>
  Layer.effect(
    Reconciler.Reconciler,
    Effect.gen(function*() {
      const configuration = yield* RetryConfiguration
      const local = yield* LocalStore.Store
      const reconciliation = yield* Reconciler.Reconciliation
      yield* Registration
      const remote = yield* SyncEngine.SyncEngine
      const engine = yield* WorkflowEngine.WorkflowEngine
      const wake = yield* Queue.sliding<void>(1)
      const activeExecution = yield* Ref.make<
        Option.Option<{
          readonly workflow: ReturnType<typeof make>
          readonly executionId: string
        }>
      >(Option.none())
      const notify = Queue.offer(wake, undefined).pipe(Effect.asVoid)
      const requestAndNotify = local.requestReconciliation.pipe(Effect.andThen(notify))
      const authenticationPause = yield* pipe(
        Option.none(),
        Ref.make<Option.Option<Deferred.Deferred<void>>>
      )
      let authenticationEpoch = 0
      const awaitAuthenticationChange = Ref.get(authenticationPause).pipe(
        Effect.flatMap(Option.match({
          onNone: () => Effect.void,
          onSome: Deferred.await
        }))
      )
      const admitCredentialPause = Effect.uninterruptible(Effect.gen(function*() {
        const candidate = yield* Deferred.make<void>()
        const admission = yield* authenticationPause.pipe(
          Ref.modify(Option.match({
            onNone: () => [{ gate: candidate, owner: true }, Option.some(candidate)],
            onSome: (gate) => [{ gate, owner: false }, Option.some(gate)]
          }))
        )
        if (admission.owner) authenticationEpoch += 1
        return admission
      }))
      const startCredentialWait = (
        generation: number,
        admission: { readonly gate: Deferred.Deferred<void>; readonly owner: boolean }
      ) => {
        if (!admission.owner) return Effect.void
        const finishWait = Effect.gen(function*() {
          const owned = yield* Ref.modify(authenticationPause, (current) => {
            if (Option.isSome(current) && current.value === admission.gate) {
              return [true, Option.none()] as const
            }
            return [false, current] as const
          })
          if (owned) yield* Deferred.succeed(admission.gate, undefined)
        }).pipe(Effect.uninterruptible)
        return remote.waitForCredentialChange(generation).pipe(
          Effect.andThen(finishWait),
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) return Effect.void
            return Effect.failCause(cause)
          }),
          Effect.forkScoped,
          Effect.asVoid
        )
      }

      const supervise = Effect.gen(function*() {
        let retryAttempt = 0
        while (true) {
          yield* Queue.take(wake)
          yield* awaitAuthenticationChange
          const result = yield* Effect.gen(function*() {
            while (true) {
              yield* awaitAuthenticationChange
              const generations = yield* local.reconciliationGenerations
              if (generations.completed >= generations.requested) return
              const state = yield* local.replicationState
              const payload = Payload.make({
                schemaIdentity: schemaIdentityKey(options.definition),
                spaceId: options.spaceId,
                clientId: options.clientId,
                membershipIncarnation: local.membershipIncarnation,
                scope: state.scope,
                scopeGeneration: state.scopeGeneration,
                generation: generations.requested
              })
              const workflow = make(payload)
              const activeExecutionId = yield* workflow.executionId(payload)
              yield* Ref.set(activeExecution, Option.some({ workflow, executionId: activeExecutionId }))
              const clearExecution = Ref.set(activeExecution, Option.none())
              yield* workflow.execute(payload).pipe(Effect.ensuring(clearExecution))
              retryAttempt = 0
            }
          }).pipe(Effect.result)
          if (Result.isSuccess(result)) continue
          const error = result.failure
          if (error._tag === "CredentialRejected") {
            if (error.credentialGeneration === undefined) {
              yield* reconciliation.failed(error)
              yield* Effect.logWarning("Rejected credential did not include its generation")
              return
            }
            const admission = yield* admitCredentialPause
            yield* reconciliation.failed(error)
            yield* startCredentialWait(error.credentialGeneration, admission)
            yield* Deferred.await(admission.gate)
            retryAttempt = 0
            yield* requestAndNotify
            continue
          }
          const pause = yield* Ref.get(authenticationPause)
          if (Option.isSome(pause)) {
            yield* Deferred.await(pause.value)
            yield* requestAndNotify
            continue
          }
          yield* reconciliation.failed(error)
          if (
            error._tag === "AuthenticatorUnavailable" ||
            error._tag === "ServerUnavailable" ||
            error._tag === "OperationTimeout"
          ) {
            retryAttempt += 1
            yield* Effect.logWarning("Reconciliation supervisor will retry", error)
            yield* Effect.sleep(Configuration.retryMillis(configuration, retryAttempt))
            yield* requestAndNotify
            continue
          }
          yield* Effect.logWarning("Reconciliation supervisor stopped", error)
          continue
        }
      })

      const supervisorFiber = yield* Effect.forkScoped(supervise)
      const watch = Effect.gen(function*() {
        let retryAttempt = 0
        while (true) {
          yield* awaitAuthenticationChange
          const watchEpoch = authenticationEpoch
          const result = yield* Stream.unwrap(Effect.map(local.replicationState, (state) =>
            remote.watch({
              spaceId: options.spaceId,
              clientId: options.clientId,
              schema: options.definition.schemaIdentity,
              scope: state.scope,
              scopeGeneration: state.scopeGeneration,
              cursor: state.cursor
            }))).pipe(
              Stream.runForEach(() => {
                retryAttempt = 0
                return requestAndNotify
              }),
              Effect.result
            )
          if (Result.isSuccess(result)) {
            retryAttempt += 1
            yield* requestAndNotify
            yield* Effect.sleep(Configuration.retryMillis(configuration, retryAttempt))
            continue
          }
          const error = result.failure
          if (watchEpoch !== authenticationEpoch) continue
          const pause = yield* Ref.get(authenticationPause)
          if (Option.isSome(pause) && error._tag !== "CredentialRejected") {
            yield* Deferred.await(pause.value)
            continue
          }
          if (error._tag === "CredentialRejected") {
            if (error.credentialGeneration === undefined) {
              yield* reconciliation.watchFailed(error)
              yield* Effect.logWarning("Rejected watch credential did not include its generation")
              return
            }
            const admission = yield* admitCredentialPause
            yield* reconciliation.watchFailed(error)
            yield* startCredentialWait(error.credentialGeneration, admission)
            yield* Deferred.await(admission.gate)
            retryAttempt = 0
            continue
          }
          yield* reconciliation.watchFailed(error)
          if (
            error._tag === "AuthenticatorUnavailable" ||
            error._tag === "ServerUnavailable" ||
            error._tag === "OperationTimeout"
          ) {
            retryAttempt += 1
            yield* Effect.logWarning("Sync watch will retry", error)
            yield* Effect.sleep(Configuration.retryMillis(configuration, retryAttempt))
            yield* notify
            continue
          }
          yield* Effect.logWarning("Sync watch stopped", error)
          return
        }
      })
      const watchFiber = yield* Effect.forkScoped(watch)
      yield* requestAndNotify
      yield* Effect.addFinalizer(() => {
        return Fiber.interruptAll([supervisorFiber, watchFiber]).pipe(
          Effect.andThen(Queue.shutdown(wake)),
          Effect.asVoid
        )
      })

      const shutdown = Effect.gen(function*() {
        const active = yield* Ref.get(activeExecution)
        const supervisorInterruption = yield* Effect.forkChild(Fiber.interrupt(supervisorFiber), {
          startImmediately: true
        })
        if (Option.isSome(active)) {
          const result = yield* engine.poll(active.value.workflow, active.value.executionId)
          if (Option.isNone(result)) {
            yield* engine.interruptUnsafe(active.value.workflow, active.value.executionId)
          }
        }
        yield* Fiber.join(supervisorInterruption)
      })
      return Reconciler.Reconciler.of({ sync: reconciliation.sync, notify, status: reconciliation.status, shutdown })
    })
  )

export const layerScheduler = (options: Options) => {
  return layerSchedulerWithConfiguration(options).pipe(Layer.provide(layerRetryConfiguration(options)))
}

export const layer = (
  options: Options
): Layer.Layer<
  Reconciler.Reconciler,
  ReplicaError.ReplicaError,
  LocalStore.Store | SyncEngine.SyncEngine | WorkflowEngine.WorkflowEngine
> => {
  const layerOnePass = Reconciler.layerOnePass(options)
  const layerConfiguration = layerRetryConfiguration(options)
  const layerConfiguredRegistration = layerRegistrationWithConfiguration(options).pipe(
    Layer.provideMerge(layerOnePass),
    Layer.provideMerge(layerConfiguration)
  )
  return layerSchedulerWithConfiguration(options).pipe(
    Layer.provideMerge(layerOnePass),
    Layer.provideMerge(layerConfiguration),
    Layer.provideMerge(layerConfiguredRegistration)
  )
}
