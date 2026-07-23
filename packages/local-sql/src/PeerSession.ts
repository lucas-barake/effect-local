import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as CommitPublisher from "./CommitPublisher.js"
import * as DocumentEntity from "./DocumentEntity.js"
import * as PeerSync from "./PeerSync.js"
import * as ReplicaGate from "./ReplicaGate.js"

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

export interface SupervisedPeerSession extends PeerSession {
  readonly awaitDisconnect: Effect.Effect<never, ReplicaError.ReplicaError>
}

export const SyncEnvelope = Schema.Struct({
  connectionEpoch: Schema.NonEmptyString,
  sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  documentId: Identity.DocumentId,
  documentType: Schema.String,
  messageHash: Schema.String,
  message: Schema.Uint8ArrayFromBase64
})
export const maximumSyncEnvelopeBytes = (maxSyncMessageBytes: number) => maxSyncMessageBytes * 2 + 4_096
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

const supervise = (
  terminalFailure: Deferred.Deferred<never, ReplicaError.ReplicaError>,
  effect: Effect.Effect<void, ReplicaError.ReplicaError>
) =>
  effect.pipe(
    Effect.tapError((error) => Deferred.fail(terminalFailure, error)),
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterruptsOnly(cause),
      (cause) =>
        Deferred.fail(
          terminalFailure,
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause })
          })
        ).pipe(Effect.asVoid)
    )
  )

const makeWithTerminal = (
  options: {
    readonly peerId: Identity.PeerId
    readonly documents: ReadonlyArray<SelectedDocument>
  },
  entity: (
    documentId: Identity.DocumentId
  ) => Effect.Effect<ReturnType<Effect.Success<typeof DocumentEntity.DocumentEntity.client>>>,
  terminalFailure: Deferred.Deferred<never, ReplicaError.ReplicaError>
): Effect.Effect<
  SupervisedPeerSession,
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
    const selected = new Set(options.documents.map((entry) => key(entry.document.name, entry.documentId)))
    const selectedDocumentIds = new Set(options.documents.map((entry) => entry.documentId))
    if (
      selected.size !== options.documents.length ||
      selectedDocumentIds.size !== options.documents.length
    ) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "unique selected documents",
          observed: String(options.documents.length)
        })
      })
    }
    const { connection, session } = yield* Effect.acquireUseRelease(
      Scope.make(),
      (scope) =>
        Effect.gen(function*() {
          const permit = yield* gate.shared.pipe(Effect.provideService(Scope.Scope, scope))
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
          return { connection, session }
        }),
      Scope.close
    )
    const dirty = yield* Ref.make(new Map<Identity.DocumentId, number>())
    const observed = yield* Ref.make(
      new Map(
        options.documents.map((entry) => [entry.documentId, { value: false, revision: 0 }])
      )
    )
    const remoteEpoch = yield* Ref.make<string | null>(null)
    const active = yield* Ref.make(true)
    const teardown = yield* Deferred.make<void>()
    const sendLock = yield* Semaphore.make(1)
    const flushLock = yield* Semaphore.make(1)
    const flushRequests = yield* Queue.dropping<void>(1)
    const scheduled = yield* Ref.make(new Map<number, PeerSync.Outbound>())
    const syncLocks = new Map(options.documents.map((entry) => [entry.documentId, Semaphore.makeUnsafe(1)]))

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
      Effect.raceFirst(
        Deferred.await(teardown),
        Effect.gen(function*() {
          if (!(yield* Ref.get(active))) return
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
            yield* sendLock.withPermit(Effect.gen(function*() {
              if (!(yield* Ref.get(active))) return
              yield* connection.send(bytes).pipe(
                Effect.timeout(limits.maxPeerSendMillis),
                Effect.catchTag("TimeoutError", (cause) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.StorageUnavailable({
                        cause
                      })
                    })
                  ))
              )
              if (yield* Ref.get(active)) {
                yield* sync.markSent(session, outbound.sendSequence, outbound.messageHash)
              }
            }))
          }))
        })
      )

    const schedule = (outbound: PeerSync.Outbound) =>
      Ref.update(scheduled, (current) => {
        const next = new Map(current)
        next.set(outbound.sendSequence, outbound)
        return next
      })

    const withSyncLock = <A, E, R,>(
      documentId: Identity.DocumentId,
      effect: Effect.Effect<A, E, R>
    ) => syncLocks.get(documentId)!.withPermit(effect)

    const drainOutbox = (afterSend: ReadonlyMap<number, Effect.Effect<void>>) =>
      Effect.gen(function*() {
        const pending = yield* Effect.raceFirst(
          Deferred.await(teardown).pipe(Effect.as([] as const)),
          sync.pending(session)
        )
        const scheduledNow = yield* Ref.getAndSet(scheduled, new Map())
        const bySequence = new Map(pending.map((outbound) => [outbound.sendSequence, outbound]))
        for (const [sendSequence, outbound] of scheduledNow) bySequence.set(sendSequence, outbound)
        const ordered = [...bySequence.values()].toSorted((left, right) => left.sendSequence - right.sendSequence)
        for (let index = 0; index < ordered.length; index++) {
          const outbound = ordered[index]!
          yield* send(outbound).pipe(
            Effect.onError(() =>
              Ref.update(scheduled, (current) => {
                const next = new Map(ordered.slice(index).map((value) => [value.sendSequence, value]))
                for (const [sendSequence, value] of current) next.set(sendSequence, value)
                return next
              })
            )
          )
          const update = afterSend.get(outbound.sendSequence)
          if (update !== undefined) yield* update
        }
        return ordered.length
      })

    const flush = flushLock.withPermit(Effect.gen(function*() {
      if (!(yield* Ref.get(active))) return
      yield* drainOutbox(new Map())
      if (!(yield* Ref.get(active))) return
      const current = yield* Ref.get(dirty)
      for (const entry of options.documents) {
        if (!(yield* Ref.get(active))) return
        const revision = current.get(entry.documentId)
        if (revision === undefined) continue
        const generated = yield* Effect.gen(function*() {
          while (true) {
            const attempt = yield* Effect.raceFirst(
              Deferred.await(teardown).pipe(
                Effect.as({ _tag: "Generated", result: null } as const)
              ),
              withSyncLock(
                entry.documentId,
                Effect.gen(function*() {
                  const observationRevision = (yield* Ref.get(observed)).get(entry.documentId)?.revision ?? 0
                  const result = yield* sync.generate(entry.document, entry.documentId, session)
                  yield* Ref.update(observed, (values) => {
                    const current = values.get(entry.documentId)
                    if ((current?.revision ?? 0) !== observationRevision) return values
                    const next = new Map(values)
                    next.set(entry.documentId, { value: result.observedByPeer, revision: observationRevision })
                    return next
                  })
                  return result
                }).pipe(
                  Effect.map((result) => ({ _tag: "Generated", result } as const)),
                  Effect.catchIf(
                    (error) =>
                      error.reason._tag === "QuotaExceeded" &&
                      (error.reason.resource === "peer sync outbox messages" ||
                        error.reason.resource === "peer sync outbox bytes"),
                    (error) => Effect.succeed({ _tag: "OutboxQuota", error } as const)
                  )
                )
              )
            )
            if (attempt._tag === "Generated") return attempt.result
            if ((yield* drainOutbox(new Map())) === 0) return yield* attempt.error
          }
        })
        if (generated === null) return
        const update = Ref.update(dirty, (values) => {
          if (values.get(entry.documentId) !== revision) return values
          const next = new Map(values)
          if (generated.dirty) next.set(entry.documentId, revision + 1)
          else next.delete(entry.documentId)
          return next
        })
        if (generated.outbound === null) yield* update
        else {
          yield* schedule(generated.outbound)
          yield* drainOutbox(new Map([[generated.outbound.sendSequence, update]]))
        }
      }
      if ((yield* Ref.get(scheduled)).size > 0) yield* drainOutbox(new Map())
    }))
    const guardTerminal = (effect: Effect.Effect<void, ReplicaError.ReplicaError>) =>
      Effect.raceFirst(Deferred.await(terminalFailure), effect).pipe(
        Effect.andThen(Deferred.isDone(terminalFailure)),
        Effect.flatMap((failed) => failed ? Deferred.await(terminalFailure) : Effect.void)
      )
    const guardedFlush = guardTerminal(flush)

    const receive = (bytes: Uint8Array) =>
      Effect.gen(function*() {
        const maximumBytes = maximumSyncEnvelopeBytes(limits.maxSyncMessageBytes)
        if (bytes.byteLength > maximumBytes) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: `sync envelope at most ${maximumBytes} bytes`,
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
        const result = yield* withSyncLock(
          envelope.documentId,
          Effect.gen(function*() {
            const incarnation = yield* Effect.scoped(Effect.gen(function*() {
              const permit = yield* gate.shared
              if (permit.incarnation !== session.replicaIncarnation) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: String(session.replicaIncarnation),
                    observed: String(permit.incarnation)
                  })
                })
              }
              return permit.incarnation
            }))
            const observationRevision = (yield* Ref.get(observed)).get(envelope.documentId)?.revision ?? 0
            const client = yield* entity(envelope.documentId)
            const result = yield* client.ApplySync({
              replicaIncarnation: incarnation,
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
                        cause
                      })
                    })
                  )
              )
            )
            yield* Ref.update(observed, (values) => {
              const current = values.get(envelope.documentId)
              if ((current?.revision ?? 0) !== observationRevision) return values
              const next = new Map(values)
              next.set(envelope.documentId, { value: result.observedByPeer, revision: observationRevision })
              return next
            })
            return result
          })
        )
        yield* publisher.publishPending
        if (result.reply !== null) {
          yield* sync.enqueue(session, result.reply).pipe(Effect.flatMap(schedule))
          yield* Queue.offer(flushRequests, undefined)
        }
      })

    yield* Effect.addFinalizer(() =>
      Effect.gen(function*() {
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
    yield* supervise(
      terminalFailure,
      Stream.runForEach(connection.receive, receive).pipe(
        Effect.andThen(
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: new Error("Peer connection receive stream ended")
              })
            })
          )
        )
      )
    ).pipe(
      Effect.ensuring(
        Ref.set(active, false).pipe(
          Effect.andThen(sendLock.withPermit(connection.close))
        )
      ),
      Effect.forkScoped
    )
    yield* Effect.addFinalizer(() =>
      Ref.set(active, false).pipe(
        Effect.andThen(Deferred.succeed(teardown, undefined)),
        Effect.andThen(flushLock.withPermit(Effect.void))
      )
    )
    yield* Deferred.await(terminalFailure).pipe(
      Effect.exit,
      Effect.andThen(
        Ref.set(active, false).pipe(
          Effect.andThen(Deferred.succeed(teardown, undefined)),
          Effect.andThen(sendLock.withPermit(connection.close))
        )
      ),
      Effect.forkScoped({ startImmediately: true })
    )
    yield* Ref.set(dirty, new Map(options.documents.map((entry) => [entry.documentId, 0])))
    yield* guardedFlush
    yield* supervise(
      terminalFailure,
      Stream.fromQueue(flushRequests).pipe(
        Stream.runForEach(() => guardedFlush)
      )
    ).pipe(
      Effect.forkScoped({ startImmediately: true })
    )

    return {
      peerId: connection.peerId,
      connectionEpoch: session.connectionEpoch,
      markDirty: (documentId) =>
        guardTerminal(
          selectedById(documentId).pipe(
            Effect.andThen(Ref.update(dirty, (current) => {
              const next = new Map(current)
              next.set(documentId, (current.get(documentId) ?? 0) + 1)
              return next
            })),
            Effect.andThen(Ref.update(observed, (current) => {
              const next = new Map(current)
              const value = current.get(documentId)
              next.set(documentId, { value: false, revision: (value?.revision ?? 0) + 1 })
              return next
            })),
            Effect.tapError((error) => Deferred.fail(terminalFailure, error))
          )
        ),
      flush: guardedFlush,
      observedByPeer: (documentId) =>
        Ref.get(observed).pipe(Effect.map((values) => values.get(documentId)?.value ?? false)),
      durableConfirmation: () => Effect.succeed(false as const),
      awaitDisconnect: Deferred.await(terminalFailure)
    }
  })

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
  Deferred.make<never, ReplicaError.ReplicaError>().pipe(
    Effect.flatMap((terminalFailure) => makeWithTerminal(options, entity, terminalFailure))
  )

export const makeSupervised = (options: {
  readonly peerId: Identity.PeerId
  readonly documents: ReadonlyArray<SelectedDocument>
}): Effect.Effect<
  SupervisedPeerSession,
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
    Effect.flatMap((entity) =>
      Deferred.make<never, ReplicaError.ReplicaError>().pipe(
        Effect.flatMap((terminalFailure) =>
          makeWithTerminal(options, (documentId) => Effect.succeed(entity(documentId)), terminalFailure)
        )
      )
    )
  )

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
> => makeSupervised(options)

export const makeLive = (options: {
  readonly peerId: Identity.PeerId
  readonly documents: ReadonlyArray<SelectedDocument>
}): Effect.Effect<
  SupervisedPeerSession,
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
  Effect.gen(function*() {
    const publisher = yield* CommitPublisher.CommitPublisher
    const subscription = yield* publisher.subscribe
    const entity = yield* DocumentEntity.DocumentEntity.client
    const terminalFailure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
    const session = yield* makeWithTerminal(
      options,
      (documentId) => Effect.succeed(entity(documentId)),
      terminalFailure
    )
    const selected = new Set(options.documents.map((entry) => entry.documentId))
    const subscriptionEnded = new ReplicaError.ReplicaError({
      reason: new ReplicaError.StorageUnavailable({
        cause: new Error("Commit subscription stream ended")
      })
    })
    const consume = Stream.runForEach(subscription.events, (event) => {
      if (event._tag === "Commit") {
        return selected.has(event.documentId)
          ? session.markDirty(event.documentId).pipe(Effect.andThen(session.flush))
          : Effect.void
      }
      return Effect.forEach(options.documents, (entry) => session.markDirty(entry.documentId), {
        discard: true
      }).pipe(Effect.andThen(session.flush))
    }).pipe(
      Effect.andThen(Effect.fail(subscriptionEnded))
    )
    yield* supervise(terminalFailure, Effect.raceFirst(Deferred.await(terminalFailure), consume)).pipe(
      Effect.forkScoped({ startImmediately: true })
    )
    return session
  })
