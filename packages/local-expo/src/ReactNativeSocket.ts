import * as Layer from "effect/Layer"
import * as Socket from "effect/socket/Socket"
import type { WebSocketConstructorOptions, WebSocketLike } from "effect/socket/Socket"

interface ReactNativeWebSocket extends WebSocketLike {
  binaryType: string
}

interface ReactNativeWebSocketConstructor {
  readonly CONNECTING: number
  new(
    url: string,
    protocols?: string | Array<string>,
    options?: { readonly headers?: Readonly<Record<string, string>> | undefined }
  ): ReactNativeWebSocket
}

const construct = (
  WebSocket: ReactNativeWebSocketConstructor,
  url: string,
  options: WebSocketConstructorOptions | undefined
) => {
  if (options === undefined || typeof options === "string" || Array.isArray(options)) {
    return new WebSocket(url, options)
  }
  return new WebSocket(url, undefined, { headers: options.headers })
}

const closeAfterOpen = (
  socket: ReactNativeWebSocket,
  code: number | undefined,
  reason: string | undefined
) => {
  const onOpen = () => {
    detach()
    socket.close(code, reason)
  }
  const onSettled = () => detach()
  const detach = () => {
    socket.removeEventListener("open", onOpen)
    socket.removeEventListener("close", onSettled)
  }
  socket.addEventListener("open", onOpen)
  socket.addEventListener("close", onSettled)
}

const wrap = (WebSocket: ReactNativeWebSocketConstructor, socket: ReactNativeWebSocket): ReactNativeWebSocket => ({
  get readyState() {
    return socket.readyState
  },
  get binaryType() {
    return socket.binaryType
  },
  set binaryType(value: string) {
    socket.binaryType = value
  },
  addEventListener: (type, listener, options) => socket.addEventListener(type, listener, options),
  removeEventListener: (type, listener) => socket.removeEventListener(type, listener),
  send: (data) => socket.send(data),
  close: (code, reason) => {
    if (socket.readyState === WebSocket.CONNECTING) closeAfterOpen(socket, code, reason)
    else socket.close(code, reason)
  }
})

export const layerWebSocketConstructor: Layer.Layer<Socket.WebSocketConstructor> = Layer.sync(
  Socket.WebSocketConstructor,
  (): Socket.WebSocketConstructor["Service"] => {
    const WebSocket: ReactNativeWebSocketConstructor = globalThis.WebSocket
    return (url, options) => wrap(WebSocket, construct(WebSocket, url, options))
  }
)
