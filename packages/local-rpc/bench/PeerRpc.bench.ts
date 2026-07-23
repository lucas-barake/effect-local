import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import type * as RpcMessage from "effect/unstable/rpc/RpcMessage"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import { bench } from "vitest"
import * as PeerRpc from "../src/PeerRpc.js"

const makeBytes = (size: number) => {
  const bytes = new Uint8Array(size)
  for (let index = 0; index < bytes.length; index++) bytes[index] = index % 251
  return bytes
}

const peerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const sessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")
const credential = Redacted.make("benchmark-credential")

const wireParsers = (envelope: RpcMessage.FromClientEncoded | RpcMessage.FromServerEncoded) => {
  const encoder = RpcSerialization.msgPack.makeUnsafe()
  const encoded = encoder.encode(envelope)
  if (!(encoded instanceof Uint8Array)) throw new Error("MessagePack encoder did not return bytes")
  const decoder = RpcSerialization.msgPack.makeUnsafe()
  if (decoder.decode(encoded).length !== 1) throw new Error("MessagePack decoder did not return one envelope")
  return { encoder, decoder, encoded }
}

const nonEmptyValues = (value: Schema.Json) => {
  if (!Array.isArray(value) || value.length === 0) throw new Error("Stream schema did not encode a non-empty array")
  return value as ReadonlyArray<unknown> as readonly [unknown, ...Array<unknown>]
}

const openPayloadCodec = Schema.toCodecJson(PeerRpc.OpenRpc.payloadSchema)
const encodeOpenPayload = Schema.encodeSync(openPayloadCodec)
const decodeOpenPayload = Schema.decodeUnknownSync(openPayloadCodec)

for (const documentCount of [1, 16, 64] as const) {
  const decoded = PeerRpc.OpenRpc.payloadSchema.make({
    protocolVersion: PeerRpc.protocolVersion,
    expectedPeerId: peerId,
    credential,
    documents: Array.from({ length: documentCount }, (_, index) => ({
      documentType: "Task",
      documentId: Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${String(index).padStart(12, "0")}`)
    }))
  })
  const encoded = encodeOpenPayload(decoded)
  const envelope: RpcMessage.RequestEncoded = {
    _tag: "Request",
    id: 1,
    tag: PeerRpc.OpenRpc._tag,
    payload: encoded,
    headers: []
  }
  const wire = wireParsers(envelope)

  bench(`Schema JSON codec Open payload encode with ${documentCount} documents`, () => {
    encodeOpenPayload(decoded)
  }, {
    iterations: 500,
    time: 0,
    warmupIterations: 50,
    warmupTime: 0
  })

  bench(`Schema JSON codec Open payload decode with ${documentCount} documents`, () => {
    decodeOpenPayload(encoded)
  }, {
    iterations: 500,
    time: 0,
    warmupIterations: 50,
    warmupTime: 0
  })

  bench(
    `MessagePack Open request envelope encode with ${documentCount} documents`,
    () => {
      wire.encoder.encode(envelope)
    },
    {
      iterations: 500,
      time: 0,
      warmupIterations: 50,
      warmupTime: 0
    }
  )

  bench(
    `MessagePack Open request envelope decode with ${documentCount} documents`,
    () => {
      wire.decoder.decode(wire.encoded)
    },
    {
      iterations: 500,
      time: 0,
      warmupIterations: 50,
      warmupTime: 0
    }
  )
}

const pushPayloadCodec = Schema.toCodecJson(PeerRpc.PushRpc.payloadSchema)
const encodePushPayload = Schema.encodeSync(pushPayloadCodec)
const decodePushPayload = Schema.decodeUnknownSync(pushPayloadCodec)
const encodeOpenChunk = Schema.encodeSync(Schema.toCodecJson(Schema.Array(PeerRpc.OpenEvent)))
const decodeOpenChunk = Schema.decodeUnknownSync(Schema.toCodecJson(Schema.NonEmptyArray(PeerRpc.OpenEvent)))

for (
  const [label, size, iterations] of [
    ["1 KiB", 1024, 500],
    ["64 KiB", 64 * 1024, 100],
    ["1 MiB", 1024 * 1024, 30]
  ] as const
) {
  const bytes = makeBytes(size)
  const decodedPush = PeerRpc.PushRpc.payloadSchema.make({ sessionId, payload: bytes, credential })
  const encodedPush = encodePushPayload(decodedPush)
  const pushEnvelope: RpcMessage.RequestEncoded = {
    _tag: "Request",
    id: 2,
    tag: PeerRpc.PushRpc._tag,
    payload: encodedPush,
    headers: []
  }
  const pushWire = wireParsers(pushEnvelope)
  const decodedChunk = [PeerRpc.Message.make({ _tag: "Message", payload: bytes })] as const
  const encodedChunk = nonEmptyValues(encodeOpenChunk(decodedChunk))
  const chunkEnvelope: RpcMessage.ResponseChunkEncoded = {
    _tag: "Chunk",
    requestId: 1,
    values: encodedChunk
  }
  const chunkWire = wireParsers(chunkEnvelope)

  bench(`Schema JSON codec Push payload encode ${label}`, () => {
    encodePushPayload(decodedPush)
  }, {
    iterations,
    time: 0,
    warmupIterations: Math.min(iterations, 20),
    warmupTime: 0
  })

  bench(`Schema JSON codec Push payload decode ${label}`, () => {
    decodePushPayload(encodedPush)
  }, {
    iterations,
    time: 0,
    warmupIterations: Math.min(iterations, 20),
    warmupTime: 0
  })

  bench(`Schema JSON codec Open Message chunk encode ${label}`, () => {
    encodeOpenChunk(decodedChunk)
  }, {
    iterations,
    time: 0,
    warmupIterations: Math.min(iterations, 20),
    warmupTime: 0
  })

  bench(`Schema JSON codec Open Message chunk decode ${label}`, () => {
    decodeOpenChunk(encodedChunk)
  }, {
    iterations,
    time: 0,
    warmupIterations: Math.min(iterations, 20),
    warmupTime: 0
  })

  bench(`MessagePack Push request envelope encode ${label}`, () => {
    pushWire.encoder.encode(pushEnvelope)
  }, {
    iterations,
    time: 0,
    warmupIterations: Math.min(iterations, 20),
    warmupTime: 0
  })

  bench(`MessagePack Push request envelope decode ${label}`, () => {
    pushWire.decoder.decode(pushWire.encoded)
  }, {
    iterations,
    time: 0,
    warmupIterations: Math.min(iterations, 20),
    warmupTime: 0
  })

  bench(`MessagePack Open stream Chunk envelope encode ${label}`, () => {
    chunkWire.encoder.encode(chunkEnvelope)
  }, {
    iterations,
    time: 0,
    warmupIterations: Math.min(iterations, 20),
    warmupTime: 0
  })

  bench(`MessagePack Open stream Chunk envelope decode ${label}`, () => {
    chunkWire.decoder.decode(chunkWire.encoded)
  }, {
    iterations,
    time: 0,
    warmupIterations: Math.min(iterations, 20),
    warmupTime: 0
  })
}

const openedEnvelope: RpcMessage.ResponseChunkEncoded = {
  _tag: "Chunk",
  requestId: 1,
  values: nonEmptyValues(encodeOpenChunk([
    PeerRpc.Opened.make({
      _tag: "Opened",
      protocolVersion: PeerRpc.protocolVersion,
      sessionId,
      peerId,
      capabilities: { storeAndForward: false }
    })
  ]))
}
const openedChunk = wireParsers(openedEnvelope)
const pushSuccessEnvelope: RpcMessage.ResponseExitEncoded = {
  _tag: "Exit",
  requestId: 2,
  exit: { _tag: "Success", value: undefined }
}
const pushSuccess = wireParsers(pushSuccessEnvelope)

bench("MessagePack Open handshake Chunk envelope encode", () => {
  openedChunk.encoder.encode(openedEnvelope)
}, {
  iterations: 500,
  time: 0,
  warmupIterations: 50,
  warmupTime: 0
})

bench("MessagePack Open handshake Chunk envelope decode", () => {
  openedChunk.decoder.decode(openedChunk.encoded)
}, {
  iterations: 500,
  time: 0,
  warmupIterations: 50,
  warmupTime: 0
})

bench("MessagePack Push success Exit envelope encode", () => {
  pushSuccess.encoder.encode(pushSuccessEnvelope)
}, {
  iterations: 500,
  time: 0,
  warmupIterations: 50,
  warmupTime: 0
})

bench("MessagePack Push success Exit envelope decode", () => {
  pushSuccess.decoder.decode(pushSuccess.encoded)
}, {
  iterations: 500,
  time: 0,
  warmupIterations: 50,
  warmupTime: 0
})
