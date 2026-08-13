import * as Effect from "effect/Effect"
import * as AttachmentWorkerProtocol from "./internal/AttachmentWorkerProtocol.js"
import * as OpfsAttachmentDirectory from "./internal/OpfsAttachmentDirectory.js"

export interface Options {
  readonly directory: string
  readonly maximumBytes: number
}

export const serveMessagePort = (port: MessagePort, options: Options) =>
  AttachmentWorkerProtocol.serve(port, { maximumBytes: options.maximumBytes }).pipe(
    Effect.provide(OpfsAttachmentDirectory.layer({ directory: options.directory }))
  )
