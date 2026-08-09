/**
 * Application lifecycle integration driven by React Native `AppState`.
 *
 * Mobile operating systems suspend backgrounded apps with no further warning,
 * so `layerFlushOnBackground` flushes the replica when the app transitions to
 * `background`: every committed mutation is then durably synced out before the
 * process can be frozen. `inactive` is deliberately ignored — it fires for
 * transient interruptions (Control Center, incoming calls) on iOS only, and
 * flushing there would pay full durability checkpoints for moments the app is
 * still alive.
 *
 * @since 0.1.0
 */
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Filter from "effect/Filter"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { AppState } from "react-native"

const AppStateStatus = Schema.Literals(["active", "background", "inactive", "extension", "unknown"])

const decodeStatus = Schema.decodeUnknownOption(AppStateStatus)

const onBackground = Filter.fromPredicateOption((status: unknown) =>
  Option.filter(decodeStatus(status), (value) => value === "background")
)

/**
 * Flushes the replica every time the app enters `background`. Flush failures
 * are logged and dropped: a suspended app cannot retry, and killing the
 * listener fiber would silently disable every later flush. The AppState
 * subscription is removed when the layer's scope closes.
 *
 * @category layers
 * @since 0.1.0
 */
export const layerFlushOnBackground: Layer.Layer<never, never, Replica.Replica> = Layer.effectDiscard(
  Effect.gen(function*() {
    const replica = yield* Replica.Replica
    const statuses = yield* Queue.unbounded<unknown>()
    const subscription = AppState.addEventListener("change", (status) => Queue.offerUnsafe(statuses, status))
    yield* Effect.addFinalizer(() => Effect.sync(() => subscription.remove()))
    yield* Stream.fromQueue(statuses).pipe(
      Stream.filterMap(onBackground),
      Stream.runForEach(() => replica.flush.pipe(Effect.tapCause(Effect.logError), Effect.ignore)),
      Effect.forkScoped
    )
  })
)
