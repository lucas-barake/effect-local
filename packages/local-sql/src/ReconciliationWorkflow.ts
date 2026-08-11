import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
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
  generation: ReconciliationGeneration
} as const

export const Payload = Schema.Struct(PayloadFields)
export type Payload = typeof Payload.Type

const schemaIdentityKey = (definition: Definition.Any): string =>
  `${definition.schemaIdentity.version}:${definition.schemaIdentity.hash}`

type ReplicaIdentity = Pick<Payload, "schemaIdentity" | "spaceId" | "clientId">

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

type EngineWithRegistrationState = WorkflowEngine.WorkflowEngine["Service"] & {
  [workflowRegistrationStateSymbol]?: WorkflowRegistrationState
}

const replicaIdentityKey = (options: Pick<Options, "spaceId" | "clientId">) =>
  JSON.stringify([options.spaceId, options.clientId])

const workflowRegistrationState = (
  engine: WorkflowEngine.WorkflowEngine["Service"]
): WorkflowRegistrationState => {
  const owned: EngineWithRegistrationState = engine
  if (owned[workflowRegistrationStateSymbol] !== undefined) return owned[workflowRegistrationStateSymbol]
  const state: WorkflowRegistrationState = { schemas: new Map() }
  Object.defineProperty(owned, workflowRegistrationStateSymbol, { value: state })
  return state
}

export const make = ({ clientId, schemaIdentity: workflowSchemaIdentity, spaceId }: ReplicaIdentity) =>
  Workflow.make(`effect-local/ReconcileReplica/v2/${JSON.stringify([workflowSchemaIdentity, spaceId, clientId])}`, {
    payload: PayloadFields,
    success: Schema.Void,
    error: ReplicaError.ReplicaError,
    idempotencyKey: (payload) =>
      JSON.stringify([payload.schemaIdentity, payload.spaceId, payload.clientId, payload.generation])
  })

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

export const executionId = (payload: Payload) => make(payload).executionId(payload)

export const start = (
  payload: Payload
): Effect.Effect<Execution, never, WorkflowEngine.WorkflowEngine> =>
  Effect.gen(function*() {
    const executionId = yield* make(payload).execute(payload, { discard: true })
    return Execution.make({ ...payload, executionId })
  })

export const validateExecution = (execution: Execution): Effect.Effect<Payload, ReplicaError.ProtocolInvalid> =>
  make(execution).executionId(execution).pipe(
    Effect.flatMap((expected) =>
      expected === execution.executionId
        ? Effect.succeed(Payload.make(execution))
        : Effect.fail(
          new ReplicaError.ProtocolInvalid({ message: "Reconciliation execution ID does not match payload" })
        )
    )
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
    const registered = registrationState.schemas.get(replicaIdentityKey(options))
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
    const local = yield* LocalStore.Store
    const reconciliation = yield* Reconciler.Reconciliation
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
          if (result.failure._tag === "StaleSchema" || attempt >= configuration.maximumAttempts) {
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

    yield* runActivity("sync", reconciliation.sync)
    yield* runActivity("complete", local.completeReconciliation(payload.generation))
    yield* reconciliation.succeeded
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
      const evolution = options.evolution ?? Evolution.make({ current: options.definition })
      const registrationState = workflowRegistrationState(engine)
      const replicaKey = replicaIdentityKey(options)
      yield* engine.register(
        make({
          schemaIdentity: schemaIdentityKey(options.definition),
          spaceId: options.spaceId,
          clientId: options.clientId
        }),
        handler(options, configuration, registrationState)
      )
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
      for (const [identity, definition] of legacySchemas) {
        yield* engine.register(
          make({
            schemaIdentity: identity,
            spaceId: options.spaceId,
            clientId: options.clientId
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
        )
      }
      const legacyDefinitions = new Map<string, Definition.Any>()
      for (const step of evolution.steps) legacyDefinitions.set(step.from.hash, step.from)
      for (const baseline of evolution.legacyBaselines) {
        legacyDefinitions.set(baseline.hash, baseline.definition)
      }
      legacyDefinitions.delete(options.definition.hash)
      for (const [definitionHash, definition] of legacyDefinitions) {
        const legacy = Workflow.make(
          `effect-local/ReconcileReplica/${JSON.stringify([definitionHash, options.spaceId, options.clientId])}`,
          {
            payload: {
              definitionHash: Schema.String,
              spaceId: Identity.SpaceId,
              clientId: Identity.ClientId,
              generation: ReconciliationGeneration
            },
            success: Schema.Void,
            error: ReplicaError.ReplicaError,
            idempotencyKey: ({ generation }) => String(generation)
          }
        )
        yield* engine.register(legacy, () =>
          Effect.gen(function*() {
            const current = registrationState.schemas.get(replicaKey) ?? options.definition.schemaIdentity
            return yield* new ReplicaError.StaleSchema({
              expectedVersion: current.version,
              expectedHash: current.hash,
              actualVersion: definition.schemaIdentity.version,
              actualHash: definition.schemaIdentity.hash
            })
          }))
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
      const wake = yield* Queue.sliding<void>(1)
      const notify = Queue.offer(wake, undefined).pipe(Effect.asVoid)
      const requestAndNotify = local.requestReconciliation.pipe(Effect.andThen(notify))

      const supervise = Effect.forever(
        Queue.take(wake).pipe(
          Effect.andThen(
            Effect.gen(function*() {
              while (true) {
                const generations = yield* local.reconciliationGenerations
                if (generations.completed >= generations.requested) return
                const payload = Payload.make({
                  schemaIdentity: schemaIdentityKey(options.definition),
                  spaceId: options.spaceId,
                  clientId: options.clientId,
                  generation: generations.requested
                })
                yield* make(payload).execute(payload)
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
          remote.watch({ spaceId: options.spaceId, schema: options.definition.schemaIdentity }).pipe(
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

      return Reconciler.Reconciler.of({ sync: reconciliation.sync, notify, status: reconciliation.status })
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
