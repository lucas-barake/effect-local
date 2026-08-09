/**
 * Application lifecycle integration driven by React Native `AppState`.
 *
 * Mobile operating systems suspend backgrounded apps with no further warning,
 * so `layerFlushOnBackground` publishes pending local replica notifications when
 * the app transitions to `background`. Relay transport remains owned by the peer
 * session and can resume later. `inactive` is deliberately ignored because it fires for
 * transient interruptions (Control Center, incoming calls) on iOS only, and
 * flushing there would pay full durability checkpoints for moments the app is
 * still alive.
 *
 * @since 0.1.0
 */
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { AppState } from "react-native"

const AppStateStatus = Schema.Literals(["active", "background", "inactive", "extension", "unknown"])

const decodeStatus = Schema.decodeUnknownOption(AppStateStatus)

/**
 * Flushes pending local replica publishers when the app enters `background`. Flush failures
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
    const backgroundRequests = yield* Queue.sliding<void>(1)
    const subscription = AppState.addEventListener("change", (status) => {
      const decoded = decodeStatus(status)
      if (Option.isSome(decoded) && decoded.value === "background") {
        Queue.offerUnsafe(backgroundRequests, undefined)
      }
    })
    yield* Effect.addFinalizer(() => Effect.sync(() => subscription.remove()))
    yield* Stream.fromQueue(backgroundRequests).pipe(
      Stream.runForEach(() => replica.flush.pipe(Effect.tapCause(Effect.logError), Effect.ignore)),
      Effect.forkScoped
    )
  })
)
