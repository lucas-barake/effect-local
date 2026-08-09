/**
 * Replica composition for React Native.
 *
 * A React Native app has exactly one JS thread and one app instance, so none
 * of the browser package's multi-tab machinery applies: there is no worker, no
 * ownership election, and no RPC boundary between the UI and the engine. The
 * replica runs in-process, composed from `SqlReplica` with an `ExpoSqlite`
 * client, and commit reactivity reaches atoms directly through the shared
 * `Reactivity` service.
 *
 * These constructors deliberately do not use `Layer.fresh`: Atom's
 * `withReactivity` subscribes to the `Reactivity` instance built under the
 * runtime's memo map, and a fresh graph would build its own instance behind
 * the memo map's back, silently disconnecting atoms from commit invalidations.
 * The price is the standard memoization rule: compose each replica once per
 * memo map, and when an app genuinely runs two replicas, give each its own
 * memo map (`Atom.context({ memoMap: Layer.makeMemoMapUnsafe() })`) instead of
 * sharing the default one.
 *
 * @since 0.1.0
 */
import type * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"

/**
 * A direct (no relay) in-process replica. Compose `ReactNativeSocket.layerWebSocket`
 * and `RelayConnectionStatus.layerProtocolSocket` separately for relay sync; see
 * {@link layerRelay}.
 *
 * @category layers
 * @since 0.1.0
 */
export const layer = <D extends ReplicaDefinition.Any, const Bindings extends ReadonlyArray<SqlProjection.Any>,>(
  definition: D,
  options: SqlReplica.Options<Bindings>
) => SqlReplica.layerWithBindings(definition, options)

/**
 * A relay-topology in-process replica. Builds on `SqlReplica.layerRelayWithBindings`
 * and deliberately leaves `RelayConnectionStatus` open: a relay replica must compose
 * `RelayConnectionStatus.layerProtocolSocket` over its socket, and leaving the
 * requirement open is what makes forgetting that a compile error.
 *
 * @category layers
 * @since 0.1.0
 */
export const layerRelay = <D extends ReplicaDefinition.Any, const Bindings extends ReadonlyArray<SqlProjection.Any>,>(
  definition: D,
  options: SqlReplica.Options<Bindings>
) => SqlReplica.layerRelayWithBindings(definition, options)
