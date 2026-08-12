import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
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

export const ReconciliationGeneration = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)
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

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))

type EngineWithRegistrationState = WorkflowEngine.WorkflowEngine["Service"] & {
  [workflowRegistrationStateSymbol]?: WorkflowRegistrationState
}

const replicaIdentityKey = (options: {
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly membershipIncarnation: Identity.MembershipIncarnation
}) => encodeJson([options.spaceId, options.clientId, options.membershipIncarnation])

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
}: ReplicaIdentity) =>
  Workflow.make(
    `effect-local/ReconcileReplica/v4/${
      encodeJson([workflowSchemaIdentity, spaceId, clientId, membershipIncarnation])
    }`,
    {
      payload: PayloadFields,
      success: Schema.Void,
      error: ReplicaError.ReplicaError,
      idempotencyKey: (payload) =>
        encodeJson([
          payload.schemaIdentity,
          payload.spaceId,
          payload.clientId,
          payload.membershipIncarnation,
          payload.scope,
          payload.scopeGeneration,
          payload.generation
        ])
    }
  )

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

export const executionId = (payload: Payload) => make(payload).executionId(payload)

export const start = (
  payload: Payload
): Effect.Effect<Execution, never, WorkflowEngine.WorkflowEngine> =>
  Effect.gen(function*() {
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
  validateExecution(execution).pipe(
    Effect.andThen(make(execution).poll(execution.executionId))
  )

export const interrupt = (execution: Execution) =>
  validateExecution(execution).pipe(
    Effect.andThen(make(execution).interrupt(execution.executionId))
  )

export const resume = (execution: Execution) =>
  validateExecution(execution).pipe(
    Effect.andThen(make(execution).resume(execution.executionId))
  )

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
      return RetryConfiguration.of({
        retryDelayMillis: yield* Configuration.positiveFiniteDurationMillis(
          "retryDelay",
          options.retryDelay ?? Duration.seconds(1)
        ),
        maximumRetryDelayMillis: yield* Configuration.positiveFiniteDurationMillis(
          "maximumRetryDelay",
          options.maximumRetryDelay ?? Duration.minutes(1)
        ),
        maximumAttempts
      })
    })
  )

const retryMillis = (configuration: RetryConfigurationService, attempt: number) =>
  Math.min(
    configuration.maximumRetryDelayMillis,
    configuration.retryDelayMillis * 2 ** Math.min(attempt - 1, 52)
  )

const handler = (
  options: Options,
  configuration: RetryConfigurationService,
  registrationState: WorkflowRegistrationState
) =>
(payload: Payload) =>
  Effect.gen(function*() {
    if (
      payload.schemaIdentity !== schemaIdentityKey(options.definition) ||
      payload.spaceId !== options.spaceId ||
      payload.clientId !== options.clientId
    ) {
      return yield* new ReplicaError.ProtocolInvalid({
        message: "Reconciliation workflow payload does not match this replica"
      })
    }
    const local = yield* LocalStore.Store
    if (payload.membershipIncarnation !== local.membershipIncarnation) {
      return yield* new ReplicaError.SpaceUnavailable({ spaceId: payload.spaceId })
    }
    const registered = registrationState.schemas.get(replicaIdentityKey({
      ...options,
      membershipIncarnation: local.membershipIncarnation
    }))
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
    const reconciliation = yield* Reconciler.Reconciliation
    const validateScope = Effect.gen(function*() {
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
    const runActivity = (name: string, execute: Effect.Effect<void, ReplicaError.ReplicaError>) =>
      Effect.gen(function*() {
        let attempt = 1
        while (true) {
          const result = yield* Activity.make({
            name: `${name}/${attempt}`,
            error: ReplicaError.ReplicaError,
            execute
          }).pipe(Effect.result)
          if (Result.isSuccess(result)) return
          yield* reconciliation.failed(result.failure)
          if (
            result.failure._tag === "StaleSchema" ||
            result.failure._tag === "SpaceUnavailable" ||
            result.failure._tag === "StaleReplicationScope" ||
            result.failure._tag === "UpgradeRequired" ||
            attempt >= configuration.maximumAttempts
          ) {
            yield* result.failure
            return
          }
          yield* DurableClock.sleep({
            name: `${name}-retry/${attempt}`,
            duration: retryMillis(configuration, attempt),
            inMemoryThreshold: Duration.zero
          })
          attempt += 1
        }
      })

    yield* runActivity("sync", validateScope.pipe(Effect.andThen(reconciliation.sync), Effect.andThen(validateScope)))
    yield* runActivity(
      "complete",
      validateScope.pipe(Effect.andThen(local.completeReconciliation(payload.generation)))
    )
    yield* reconciliation.succeeded
    return undefined
  })

const layerRegistrationWithConfiguration = (
  options: Options
): Layer.Layer<
  Registration,
  ReplicaError.InvalidConfiguration,
  LocalStore.Store | Reconciler.Reconciliation | RetryConfiguration | WorkflowEngine.WorkflowEngine
> =>
  Layer.unwrap(
    Effect.gen(function*() {
      const configuration = yield* RetryConfiguration
      const engine = yield* WorkflowEngine.WorkflowEngine
      const local = yield* LocalStore.Store
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
      const replicaKey = replicaIdentityKey(identity)
      yield* engine.register(
        make({
          schemaIdentity: schemaIdentityKey(options.definition),
          spaceId: options.spaceId,
          clientId: options.clientId,
          membershipIncarnation: local.membershipIncarnation
        }),
        handler(options, configuration, registrationState)
      ).pipe(Scope.provide(registrationScope))
      const registered = registrationState.schemas.get(replicaKey)
      if (
        registered !== undefined &&
        registered.version === options.definition.schemaIdentity.version &&
        registered.hash !== options.definition.schemaIdentity.hash
      ) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "definition",
          message:
            `Reconciliation workflow schema version ${registered.version} is already registered with another hash`
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
          () =>
            Effect.gen(function*() {
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
      return Layer.succeed(Registration, Registration.of({ registered: true }))
    })
  )

export const layerRegistration = (options: Options) =>
  layerRegistrationWithConfiguration(options).pipe(Layer.provide(layerRetryConfiguration(options)))

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

      const supervise = Effect.forever(
        Queue.take(wake).pipe(
          Effect.andThen(
            Effect.gen(function*() {
              while (true) {
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
                yield* workflow.execute(payload).pipe(
                  Effect.ensuring(Ref.set(activeExecution, Option.none()))
                )
              }
            })
          ),
          Effect.catch((error) =>
            reconciliation.failed(error).pipe(
              Effect.andThen(Effect.logWarning("Reconciliation supervisor failed", error))
            )
          )
        )
      )

      const supervisorFiber = yield* Effect.forkScoped(supervise)
      const watchFiber = yield* Effect.forkScoped(
        Effect.forever(
          local.replicationState.pipe(
            Effect.map((state) => ({
              spaceId: options.spaceId,
              clientId: options.clientId,
              schema: options.definition.schemaIdentity,
              scope: state.scope,
              scopeGeneration: state.scopeGeneration,
              cursor: state.cursor
            })),
            Effect.map(remote.watch),
            Stream.unwrap,
            Stream.runForEach(() => requestAndNotify),
            Effect.matchEffect({
              onFailure: (error) => {
                if (error._tag === "StaleSchema") return Effect.fail(error)
                return Effect.logWarning("Sync watch ended", error).pipe(Effect.andThen(notify))
              },
              onSuccess: () => requestAndNotify
            }),
            Effect.andThen(Effect.sleep(configuration.retryDelayMillis))
          )
        ).pipe(Effect.catchTag("StaleSchema", reconciliation.failed))
      )
      yield* requestAndNotify
      yield* Effect.addFinalizer(() =>
        Fiber.interruptAll([supervisorFiber, watchFiber]).pipe(
          Effect.andThen(Queue.shutdown(wake)),
          Effect.asVoid
        )
      )

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

export const layerScheduler = (options: Options) =>
  layerSchedulerWithConfiguration(options).pipe(Layer.provide(layerRetryConfiguration(options)))

export const layer = (
  options: Options
): Layer.Layer<
  Reconciler.Reconciler,
  ReplicaError.ReplicaError,
  LocalStore.Store | SyncEngine.SyncEngine | WorkflowEngine.WorkflowEngine
> => {
  const onePass = Reconciler.layerOnePass(options)
  const configuration = layerRetryConfiguration(options)
  const registration = layerRegistrationWithConfiguration(options).pipe(
    Layer.provideMerge(onePass),
    Layer.provideMerge(configuration)
  )
  return layerSchedulerWithConfiguration(options).pipe(
    Layer.provideMerge(onePass),
    Layer.provideMerge(configuration),
    Layer.provideMerge(registration)
  )
}
