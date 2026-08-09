/**
 * WebSocket layers for Effect sockets on React Native.
 *
 * React Native ships a spec-shaped global `WebSocket`, so the core Effect
 * socket constructor works unmodified; this module only wires the global
 * constructor service, mirroring `@effect/platform-browser/BrowserSocket`.
 *
 * One React Native caveat: inbound binary frames are delivered as
 * `ArrayBuffer`, not `Uint8Array` (the native bridge base64-decodes them into
 * an ArrayBuffer). Effect Local's relay speaks JSON text frames, so the relay
 * composition is unaffected; binary protocols must wrap `event.data`
 * themselves.
 *
 * @since 0.1.0
 */
import * as Layer from "effect/Layer"
import * as Socket from "effect/unstable/socket/Socket"

/**
 * Creates a `Socket` layer connected to the given URL using the React Native
 * global `WebSocket` constructor.
 *
 * @category layers
 * @since 0.1.0
 */
export const layerWebSocket = (url: string, options?: {
  readonly closeCodeIsError?: (code: number) => boolean
}): Layer.Layer<Socket.Socket> =>
  Layer.effect(Socket.Socket, Socket.makeWebSocket(url, options)).pipe(
    Layer.provide(Socket.layerWebSocketConstructorGlobal)
  )

/**
 * Layer that provides a `WebSocketConstructor` service backed by the React
 * Native global `WebSocket`.
 *
 * @category layers
 * @since 0.1.0
 */
export const layerWebSocketConstructor: Layer.Layer<Socket.WebSocketConstructor> =
  Socket.layerWebSocketConstructorGlobal
