import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as DocumentEntity from "@lucas-barake/effect-local-sql/DocumentEntity"
import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
import * as ReplicaGate from "@lucas-barake/effect-local-sql/ReplicaGate"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import type * as Sharding from "effect/unstable/cluster/Sharding"

export interface SelectedDocument {
  readonly document: Document.Any
  readonly documentId: Identity.DocumentId
}

export interface PeerSession {
  readonly peerId: Identity.PeerId
  readonly connectionEpoch: string
  readonly markDirty: (documentId: Identity.DocumentId) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly flush: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly observedByPeer: (documentId: Identity.DocumentId) => Effect.Effect<boolean>
  readonly durableConfirmation: (documentId: Identity.DocumentId) => Effect.Effect<false>
}

export const SyncEnvelope = Schema.Struct({
  connectionEpoch: Schema.NonEmptyString,
  sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  documentId: Identity.DocumentId,
  documentType: Schema.String,
  messageHash: Schema.String,
  message: Schema.Uint8ArrayFromBase64
})
const SyncEnvelopeJson = Schema.fromJsonString(Schema.toCodecJson(SyncEnvelope))

const key = (documentType: string, documentId: Identity.DocumentId) => `${documentType}:${documentId}`

const encode = (envelope: typeof SyncEnvelope.Type) =>
  Schema.encodeEffect(SyncEnvelopeJson)(envelope).pipe(
    Effect.map((value) => new TextEncoder().encode(value)),
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "encodable sync envelope",
          observed: String(cause)
        })
      })
    )
  )

const decode = (bytes: Uint8Array) =>
  Schema.decodeUnknownEffect(SyncEnvelopeJson)(new TextDecoder().decode(bytes)).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "sync envelope",
          observed: String(cause)
        })
      })
    )
  )

export const makeTestClient = (
  options: {
    readonly peerId: Identity.PeerId
    readonly documents: ReadonlyArray<SelectedDocument>
  },
  entity: (
    documentId: Identity.DocumentId
  ) => Effect.Effect<ReturnType<Effect.Success<typeof DocumentEntity.DocumentEntity.client>>>
): Effect.Effect<
  PeerSession,
  ReplicaError.ReplicaError,
  | Scope.Scope
  | CommitPublisher.CommitPublisher
  | Crypto.Crypto
  | PeerTransport.PeerTransport
  | PeerSync.PeerSync
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
> =>
  Effect.gen(function*() {
    const gate = yield* ReplicaGate.ReplicaGate
    const publisher = yield* CommitPublisher.CommitPublisher
    const limits = yield* ReplicaLimits.ReplicaLimits
    const transport = yield* PeerTransport.PeerTransport
    const sync = yield* PeerSync.PeerSync
    const crypto = yield* Crypto.Crypto
    const permit = yield* Effect.scoped(gate.shared)
    const connection = yield* transport.connect({ replicaId: permit.replicaId, peerId: options.peerId })
    const session = yield* sync.open(connection.peerId)
    if (session.replicaIncarnation !== permit.incarnation) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: String(permit.incarnation),
          observed: String(session.replicaIncarnation)
        })
      })
    }
    const selected = new Set(options.documents.map((entry) => key(entry.document.name, entry.documentId)))
    if (selected.size !== options.documents.length) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "unique selected documents",
          observed: String(options.documents.length)
        })
      })
    }
    const dirty = yield* Ref.make(new Map<Identity.DocumentId, number>())
    const observed = yield* Ref.make(new Map<Identity.DocumentId, boolean>())
    const remoteEpoch = yield* Ref.make<string | null>(null)
    const active = yield* Ref.make(true)
    const receiveFailure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
    const sendLock = yield* Semaphore.make(1)
    const flushLock = yield* Semaphore.make(1)

    const selectedById = (documentId: Identity.DocumentId) => {
      const entry = options.documents.find((candidate) => candidate.documentId === documentId)
      return entry === undefined
        ? Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "selected document",
              observed: documentId
            })
          })
        )
        : Effect.succeed(entry)
    }

    const send = (outbound: PeerSync.Outbound) =>
      sendLock.withPermit(Effect.gen(function*() {
        const entry = yield* selectedById(outbound.documentId)
        const bytes = yield* encode({
          connectionEpoch: session.connectionEpoch,
          sequence: outbound.sendSequence,
          documentId: outbound.documentId,
          documentType: entry.document.name,
          messageHash: outbound.messageHash,
          message: outbound.message
        })
        yield* Effect.scoped(Effect.gen(function*() {
          const permit = yield* gate.shared
          if (permit.incarnation !== session.replicaIncarnation) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: String(session.replicaIncarnation),
                observed: String(permit.incarnation)
              })
            })
          }
          yield* connection.send(bytes).pipe(
            Effect.timeout(limits.maxPeerSendMillis),
            Effect.catchTag("TimeoutError", (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: new ReplicaError.RpcCause({ message: String(cause) })
                  })
                })
              ))
          )
        }))
        if (yield* Ref.get(active)) {
          yield* sync.markSent(session, outbound.sendSequence, outbound.messageHash)
        }
      }))

    const flush = flushLock.withPermit(Effect.gen(function*() {
      for (const outbound of yield* sync.pending(session)) yield* send(outbound)
      const current = yield* Ref.get(dirty)
      for (const entry of options.documents) {
        const revision = current.get(entry.documentId)
        if (revision === undefined) continue
        const generated = yield* sync.generate(entry.document, entry.documentId, session)
        if (generated.outbound !== null) yield* send(generated.outbound)
        yield* Ref.update(observed, (values) => {
          const next = new Map(values)
          next.set(entry.documentId, generated.observedByPeer)
          return next
        })
        yield* Ref.update(dirty, (values) => {
          if (values.get(entry.documentId) !== revision) return values
          const next = new Map(values)
          if (generated.dirty) next.set(entry.documentId, revision + 1)
          else next.delete(entry.documentId)
          return next
        })
      }
    }))

    const receive = (bytes: Uint8Array) =>
      Effect.gen(function*() {
        const { envelope, result } = yield* Effect.scoped(Effect.gen(function*() {
          const permit = yield* gate.shared
          if (permit.incarnation !== session.replicaIncarnation) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: String(session.replicaIncarnation),
                observed: String(permit.incarnation)
              })
            })
          }
          if (bytes.byteLength > limits.maxSyncMessageBytes * 2 + 4_096) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: `sync envelope at most ${limits.maxSyncMessageBytes * 2 + 4096} bytes`,
                observed: String(bytes.byteLength)
              })
            })
          }
          const envelope = yield* decode(bytes)
          const boundEpoch = yield* Ref.modify(
            remoteEpoch,
            (current) => current === null ? [envelope.connectionEpoch, envelope.connectionEpoch] : [current, current]
          )
          if (boundEpoch !== envelope.connectionEpoch) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: boundEpoch,
                observed: envelope.connectionEpoch
              })
            })
          }
          const messageHash = yield* Canonical.digest(envelope.message).pipe(
            Effect.provideService(Crypto.Crypto, crypto)
          )
          if (messageHash !== envelope.messageHash) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: messageHash,
                observed: envelope.messageHash
              })
            })
          }
          if (!selected.has(key(envelope.documentType, envelope.documentId))) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "selected whole document",
                observed: `${envelope.documentType}:${envelope.documentId}`
              })
            })
          }
          const client = yield* entity(envelope.documentId)
          const result = yield* client.ApplySync({
            replicaIncarnation: permit.incarnation,
            peerId: connection.peerId,
            connectionEpoch: boundEpoch,
            localConnectionEpoch: session.connectionEpoch,
            receiveSequence: envelope.sequence,
            documentType: envelope.documentType,
            messageHash: envelope.messageHash,
            message: envelope.message
          }).pipe(
            Effect.catchTag(
              ["MailboxFull", "AlreadyProcessingMessage", "PersistenceError"],
              (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause: new ReplicaError.RpcCause({ message: String(cause) })
                    })
                  })
                )
            )
          )
          return { envelope, result }
        }))
        yield* publisher.publishPending
        yield* Ref.update(observed, (values) => {
          const next = new Map(values)
          next.set(envelope.documentId, result.observedByPeer)
          return next
        })
        if (result.reply !== null) {
          yield* sync.enqueue(session, result.reply).pipe(Effect.flatMap(send))
        }
      })

    yield* Effect.addFinalizer(() =>
      Effect.gen(function*() {
        yield* Ref.set(active, false)
        const boundEpoch = yield* Ref.get(remoteEpoch)
        yield* sync.reset(session).pipe(
          Effect.ensuring(
            boundEpoch === null
              ? Effect.void
              : sync.reset({
                peerId: connection.peerId,
                connectionEpoch: boundEpoch,
                replicaIncarnation: session.replicaIncarnation
              }).pipe(Effect.orDie)
          )
        )
      }).pipe(
        Effect.ensuring(connection.close),
        Effect.orDie
      )
    )
    yield* Stream.runForEach(connection.receive, receive).pipe(
      Effect.andThen(
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({
              cause: new ReplicaError.RpcCause({ message: "Peer connection receive stream ended" })
            })
          })
        )
      ),
      Effect.tapError((error) => Deferred.fail(receiveFailure, error)),
      Effect.ensuring(connection.close),
      Effect.forkScoped
    )
    yield* Ref.set(dirty, new Map(options.documents.map((entry) => [entry.documentId, 0])))
    yield* Effect.raceFirst(flush, Deferred.await(receiveFailure))

    return {
      peerId: connection.peerId,
      connectionEpoch: session.connectionEpoch,
      markDirty: (documentId) =>
        Effect.raceFirst(
          selectedById(documentId).pipe(
            Effect.andThen(Ref.update(dirty, (current) => {
              const next = new Map(current)
              next.set(documentId, (current.get(documentId) ?? 0) + 1)
              return next
            }))
          ),
          Deferred.await(receiveFailure)
        ),
      flush: Effect.raceFirst(flush, Deferred.await(receiveFailure)),
      observedByPeer: (documentId) => Ref.get(observed).pipe(Effect.map((values) => values.get(documentId) ?? false)),
      durableConfirmation: () => Effect.succeed(false as const)
    }
  })

export const make = (options: {
  readonly peerId: Identity.PeerId
  readonly documents: ReadonlyArray<SelectedDocument>
}): Effect.Effect<
  PeerSession,
  ReplicaError.ReplicaError,
  | Scope.Scope
  | CommitPublisher.CommitPublisher
  | Crypto.Crypto
  | PeerTransport.PeerTransport
  | PeerSync.PeerSync
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
  | Sharding.Sharding
> =>
  DocumentEntity.DocumentEntity.client.pipe(
    Effect.flatMap((entity) => makeTestClient(options, (documentId) => Effect.succeed(entity(documentId))))
  )
