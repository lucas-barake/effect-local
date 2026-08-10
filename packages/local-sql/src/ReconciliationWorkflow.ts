import type * as Definition from "@lucas-barake/effect-local/Definition"
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
  definitionHash: Schema.String,
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  generation: ReconciliationGeneration
} as const

export const Payload = Schema.Struct(PayloadFields)
export type Payload = typeof Payload.Type

type ReplicaIdentity = Pick<Payload, "definitionHash" | "spaceId" | "clientId">

export const make = ({ clientId, definitionHash, spaceId }: ReplicaIdentity) =>
  Workflow.make(`effect-local/ReconcileReplica/${JSON.stringify([definitionHash, spaceId, clientId])}`, {
    payload: PayloadFields,
    success: Schema.Void,
    error: ReplicaError.ReplicaError,
    idempotencyKey: ({ generation }) => String(generation)
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

const handler = (options: Options, configuration: RetryConfigurationService) => (payload: Payload) =>
  Effect.gen(function*() {
    if (
      payload.definitionHash !== options.definition.hash ||
      payload.spaceId !== options.spaceId ||
      payload.clientId !== options.clientId
    ) {
      return yield* new ReplicaError.ProtocolInvalid({
        message: "Reconciliation workflow payload does not match this replica"
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
          if (attempt >= configuration.maximumAttempts) return yield* result.failure
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
  never,
  LocalStore.Store | Reconciler.Reconciliation | RetryConfiguration | WorkflowEngine.WorkflowEngine
> =>
  Layer.unwrap(
    Effect.gen(function*() {
      const configuration = yield* RetryConfiguration
      const engine = yield* WorkflowEngine.WorkflowEngine
      yield* engine.register(
        make({
          definitionHash: options.definition.hash,
          spaceId: options.spaceId,
          clientId: options.clientId
        }),
        handler(options, configuration)
      )
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
                  definitionHash: options.definition.hash,
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
          remote.watch(options.spaceId).pipe(
            Stream.runForEach(() => requestAndNotify),
            Effect.andThen(requestAndNotify),
            Effect.catch((error) => Effect.logWarning("Sync watch ended", error)),
            Effect.ensuring(notify),
            Effect.andThen(Effect.sleep(configuration.retryDelayMillis))
          )
        )
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
