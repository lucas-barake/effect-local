import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type * as Document from "./Document.js"
import type * as Identity from "./Identity.js"
import * as SchemaInput from "./internal/schemaInput.js"
import * as ReplicaError from "./ReplicaError.js"

export const TypeId: unique symbol = Symbol.for("@lucas-barake/effect-local/Transient")
export type TypeId = typeof TypeId

export interface Delivery {
  readonly peerId: Identity.PeerId
  readonly documentId: Identity.DocumentId
  readonly payload: Uint8Array
}

export class Transport extends Context.Service<Transport, {
  readonly send: (
    peerId: Identity.PeerId,
    documentId: Identity.DocumentId,
    payload: Uint8Array
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly messages: Stream.Stream<Delivery, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local/Transient/Transport") {}

export interface Message<A,> {
  readonly peerId: Identity.PeerId
  readonly payload: A
}

export interface Client<A,> {
  readonly publish: (
    peerId: Identity.PeerId,
    payload: A
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly messages: Stream.Stream<Message<A>, ReplicaError.ReplicaError>
}

export interface Topic<
  out Name extends string,
  D extends Document.Any,
  P extends Document.WireSchema,
> {
  readonly [TypeId]: TypeId
  readonly name: Name
  readonly document: D
  readonly payloadSchema: P
  readonly client: Effect.Effect<
    (documentId: Identity.DocumentId) => Client<P["Type"]>,
    never,
    Transient
  >
}

export interface Any {
  readonly [TypeId]: TypeId
  readonly name: string
  readonly document: Document.Any
  readonly payloadSchema: Document.WireSchema
}

export class Transient extends Context.Service<Transient, {
  readonly client: <A,>(topic: Any, documentId: Identity.DocumentId) => Client<A>
}>()("@lucas-barake/effect-local/Transient") {}

export const make = <
  const Name extends string,
  D extends Document.Any,
  P extends SchemaInput.Input,
>(
  name: Name,
  options: {
    readonly document: D
    readonly payload: SchemaInput.Valid<P>
  }
): Topic<Name, D, SchemaInput.Wire<P>> => {
  if (name.length === 0) throw new TypeError("Transient name must be nonempty")
  if (name.startsWith("$")) {
    throw new TypeError(`Transient name must not start with "$", it is reserved for protocol sentinels: ${name}`)
  }
  const topic: Topic<Name, D, SchemaInput.Wire<P>> = {
    [TypeId]: TypeId,
    name,
    document: options.document,
    payloadSchema: SchemaInput.normalize(options.payload),
    client: Transient.pipe(
      Effect.map((service) => (documentId: Identity.DocumentId) => service.client(topic, documentId))
    )
  }
  return topic
}

const Envelope = Schema.Struct({
  topic: Schema.String,
  payload: Schema.Json
})
const EnvelopeJson = Schema.fromJsonString(Schema.toCodecJson(Envelope))
const encoder = new TextEncoder()
const decoder = new TextDecoder()

export const layer = (topics: ReadonlyArray<Any>): Layer.Layer<Transient, never, Transport> =>
  Layer.effect(
    Transient,
    Transport.pipe(Effect.map((transport) => {
      const registered = new Set(topics)
      return Transient.of({
        client: <A,>(topic: Any, documentId: Identity.DocumentId): Client<A> => {
          if (!registered.has(topic)) throw new TypeError(`Transient is not registered: ${topic.name}`)
          const payloadCodec = Schema.toCodecJson(topic.payloadSchema)
          return {
            publish: (peerId, payload) =>
              Schema.encodeUnknownEffect(payloadCodec)(payload).pipe(
                Effect.flatMap((encodedPayload) =>
                  Schema.encodeEffect(EnvelopeJson)({ topic: topic.name, payload: encodedPayload })
                ),
                Effect.mapError((cause) =>
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.TransientEncodeError({ topic: topic.name, documentId, cause })
                  })
                ),
                Effect.flatMap((encoded) => transport.send(peerId, documentId, encoder.encode(encoded)))
              ),
            messages: transport.messages.pipe(
              Stream.filter((message) => message.documentId === documentId),
              Stream.mapEffect((message) =>
                Schema.decodeUnknownEffect(EnvelopeJson)(decoder.decode(message.payload)).pipe(
                  Effect.flatMap((envelope) =>
                    envelope.topic === topic.name
                      ? Schema.decodeUnknownEffect(payloadCodec)(envelope.payload).pipe(
                        Effect.map((decoded) => ({ peerId: message.peerId, payload: decoded as A }))
                      )
                      : Effect.succeed(undefined)
                  ),
                  Effect.mapError((cause) =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.TransientDecodeError({ topic: topic.name, documentId, cause })
                    })
                  )
                )
              ),
              Stream.filter((message): message is Message<A> => message !== undefined)
            )
          }
        }
      })
    }))
  )
