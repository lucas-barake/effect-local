import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Clock from "effect/Clock"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { encodeInboxKey } from "./internal/relayInboxKey.js"
import * as PeerAuthentication from "./PeerAuthentication.js"
import * as PeerRelayAuthorization from "./PeerRelayAuthorization.js"
import * as PeerRelayLimits from "./PeerRelayLimits.js"
import * as PeerRpc from "./PeerRpc.js"
import * as PeerRpcError from "./PeerRpcError.js"
import * as RelayInbox from "./RelayInbox.js"
import * as RelayInboxMaintenance from "./RelayInboxMaintenance.js"

/**
 * The relay's public front door.
 *
 * Terminates the client socket, authenticates and authorizes each request, and forwards it to the
 * cluster entity that owns the addressed device. It holds no custody of its own: every durable
 * decision belongs to an entity, so any node can serve any client and a node failure costs only the
 * connections it was terminating.
 *
 * Handshake state is deliberately per connection rather than in the cluster. A socket lives on
 * exactly one node and dies with it, so a client that loses this node simply reconnects and opens
 * again. What could not be process local is the *recipient's* delivery session, because a message
 * accepted anywhere has to reach whichever node holds that recipient's socket — and that is what
 * the entity provides.
 */

export interface Options {
  /** The tenant this relay serves. Requests authenticated into any other tenant are refused. */
  readonly tenantId: string
  /** The relay's own peer identity, which clients pin at handshake. */
  readonly peerId: Identity.PeerId
  /**
   * How often a live session is kept alive with the entity that owns it.
   *
   * Driven from here rather than by the client, because what has to be proven alive is this node
   * and its socket, not the peer at the other end.
   */
  readonly heartbeatIntervalMillis: number
  /**
   * How long to wait on an entity call before giving up.
   *
   * Sharding retries `EntityNotAssignedToRunner` and `RunnerUnavailable` indefinitely, so without a
   * bound a request during a rebalance would hang forever and leak a fiber per in-flight call.
   */
  readonly entityCallTimeoutMillis: number
}

/** Everything the handshake established, held for the life of one client connection. */
interface Session {
  readonly sessionId: Identity.SessionId
  readonly principal: PeerAuthentication.PeerPrincipal
  readonly remote: PeerAuthentication.PeerPrincipal
  readonly documents: ReadonlyArray<PeerRpc.RequestedDocument>
  readonly senderReplicaIncarnation: Identity.ReplicaIncarnation
  readonly senderRetryHorizonMillis: number
  readonly inboxKeySelf: string
  readonly inboxKeyRemote: string
}

const RelayEnvelopeJson = Schema.fromJsonString(
  Schema.toCodecJson(PeerSyncEnvelope.SyncEnvelope)
)

export const layerHandlers = (options: Options) =>
  PeerRpc.Rpcs.toLayer(Effect.gen(function*() {
    const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization
    const limits = yield* PeerRelayLimits.PeerRelayLimits
    const inboxClient = yield* RelayInbox.RelayInbox.client
    // Resolved here so no handler carries a service in its own requirement channel.
    const crypto = yield* Crypto.Crypto

    const sessions = new Map<Identity.SessionId, Session>()
    const decodeEnvelope = Schema.decodeUnknownEffect(RelayEnvelopeJson)

    /**
     * Bounds an entity call and reports exhaustion as unavailability.
     *
     * Applied at each call site rather than wrapped around the client, so the timeout is visible
     * where the request is made.
     */
    const bounded = <A, E, R,>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.timeoutOrElse({
          duration: options.entityCallTimeoutMillis,
          orElse: () => Effect.fail(new PeerRpcError.ServerUnavailable())
        })
      )

    /**
     * Resolves the caller's session.
     *
     * A session the front door does not know is reported as unavailable rather than as an
     * authentication problem: the credential may be perfectly valid and simply belong to a session
     * this node never opened or has already replaced.
     */
    const sessionFor = (sessionId: Identity.SessionId, principal: PeerAuthentication.PeerPrincipal) =>
      Effect.suspend(() => {
        const session = sessions.get(sessionId)
        if (session === undefined) return Effect.fail(new PeerRpcError.SessionUnavailable())
        // A session id is unguessable, but it must still belong to the caller presenting it.
        if (
          session.principal.tenantId !== principal.tenantId ||
          session.principal.subjectId !== principal.subjectId ||
          session.principal.peerId !== principal.peerId
        ) {
          return Effect.fail(new PeerRpcError.SessionUnavailable())
        }
        return Effect.succeed(session)
      })

    /**
     * Hands a withheld delivery back to the entity.
     *
     * Best effort: if it does not land, the channel stays held until the session ends, which is the
     * behaviour this call exists to avoid but is not itself a reason to fail the recipient's whole
     * stream over a message it was never going to see.
     */
    const release = (
      inboxKey: string,
      sessionId: Identity.SessionId,
      message: PeerRpc.StoredMessage
    ) =>
      Effect.ignore(bounded(
        inboxClient(inboxKey).Release({
          sessionId,
          relayMessageId: message.relayMessageId,
          claimToken: message.claimToken
        })
      ))

    const settle = (
      payload: {
        readonly sessionId: Identity.SessionId
        readonly relayMessageId: Identity.RelayMessageId
        readonly claimToken: PeerRpc.ClaimToken
        readonly messageHash: string
      },
      outcome: "Acknowledged" | "Rejected"
    ) =>
      Effect.gen(function*() {
        const authenticated = yield* PeerAuthentication.AuthenticatedPeer
        const session = yield* sessionFor(payload.sessionId, authenticated.principal)

        // Re-authorized here, not merely at the handshake. Settling is a durable mutation of the
        // relay's state, and a principal whose Receive authority has been withdrawn must not be
        // able to perform one. The session monitor also ends a session whose grants lapse, but that
        // is asynchronous, so on its own it leaves a window in which a revoked peer can still
        // settle — and it reports the wrong thing, `SessionUnavailable` rather than `AccessDenied`.
        const receive = yield* authorization.authorize({
          direction: "Receive",
          principal: session.principal,
          remote: session.remote,
          documents: session.documents
        })
        if (receive.documents.length === 0) {
          return yield* new PeerRpcError.AccessDenied()
        }
        yield* authorization.authorizeUnsafeUnboundedAutomerge3Decode({
          risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
          direction: "Receive",
          principal: session.principal,
          remote: session.remote,
          documents: session.documents
        })

        // Keyed by the caller's own inbox, so settling another device's message is not expressible.
        yield* bounded(
          inboxClient(session.inboxKeySelf).Settle({
            sessionId: payload.sessionId,
            relayMessageId: payload.relayMessageId,
            claimToken: payload.claimToken,
            messageHash: payload.messageHash,
            outcome
          })
        ).pipe(
          Effect.catchTag("MailboxFull", () => new PeerRpcError.SessionOverloaded()),
          Effect.catchTag("AlreadyProcessingMessage", () => new PeerRpcError.SessionOverloaded()),
          Effect.catchTag("PersistenceError", () => new PeerRpcError.ServerUnavailable())
        )
      }).pipe(
        // Identifiers only. The session id is unguessable but not secret, and the message hash is
        // already a digest; the payload is never an attribute.
        Effect.withSpan("RelayServer.Settle", {
          attributes: { relay_message_id: payload.relayMessageId, outcome }
        })
      )

    return PeerRpc.Rpcs.of({
      Open: (payload) =>
        // Spans the handshake, not the session: the stream it returns outlives this effect.
        Stream.unwrap(
          Effect.withSpan("RelayServer.Open", {
            attributes: { remote_peer_id: payload.remote.peerId }
          })(Effect.gen(function*() {
            const authenticated = yield* PeerAuthentication.AuthenticatedPeer
            const principal = authenticated.principal
            const now = yield* Clock.currentTimeMillis

            if (authenticated.validUntil <= now) {
              return yield* new PeerRpcError.AuthenticationFailure()
            }
            if (payload.protocolVersion !== PeerRpc.protocolVersion) {
              return yield* new PeerRpcError.UnsupportedVersion()
            }
            // The client states who it believes it is and which relay it believes it reached. Both
            // are checked against the authenticated identity rather than trusted, so a credential can
            // never be used to open a session for another device.
            if (
              payload.expectedRelayPeerId !== options.peerId ||
              payload.expectedLocal.tenantId !== principal.tenantId ||
              payload.expectedLocal.subjectId !== principal.subjectId ||
              payload.expectedLocal.peerId !== principal.peerId
            ) {
              return yield* new PeerRpcError.PeerMismatch()
            }
            if (principal.tenantId !== options.tenantId) {
              return yield* new PeerRpcError.AccessDenied()
            }
            // The negotiated windows have to nest: a receipt must outlive both the message and the
            // window in which its sender may replay it, or a redelivery could be applied twice.
            if (
              payload.senderRetryHorizonMillis > limits.maximumSenderRetryHorizonMillis ||
              payload.receiptRetentionMillis > limits.maximumReceiptRetentionMillis ||
              payload.receiptRetentionMillis <
                Math.max(limits.messageTtlMillis, payload.senderRetryHorizonMillis) +
                  limits.minimumTerminalRetentionMillis
            ) {
              return yield* new PeerRpcError.InvalidRequest()
            }

            const send = yield* authorization.authorize({
              direction: "Send",
              principal,
              remote: payload.remote,
              documents: payload.documents
            })
            const receive = yield* authorization.authorize({
              direction: "Receive",
              principal,
              remote: payload.remote,
              documents: payload.documents
            })
            // Authorization is not instantaneous, so both grants and the credential are rechecked
            // against the clock afterwards rather than against the time the request arrived.
            const authorizedAt = yield* Clock.currentTimeMillis
            if (authenticated.validUntil <= authorizedAt) {
              return yield* new PeerRpcError.AuthenticationFailure()
            }
            if (send.validUntil <= authorizedAt || receive.validUntil <= authorizedAt) {
              return yield* new PeerRpcError.AccessDenied()
            }

            // Sessions are the front door's only unbounded resource, and an authenticated client can
            // open them in a loop. Checked before minting an id so a refusal costs nothing.
            let heldBySubject = 0
            for (const held of sessions.values()) {
              if (
                held.principal.tenantId === principal.tenantId &&
                held.principal.subjectId === principal.subjectId
              ) {
                heldBySubject += 1
              }
            }
            if (heldBySubject >= limits.maxSessionsPerSubject) {
              return yield* new PeerRpcError.RequestCapacityExceeded()
            }

            const sessionId = yield* Identity.makeSessionId.pipe(
              Effect.provideService(Crypto.Crypto, crypto),
              // The platform's randomness failing is an environment fault, not something the client
              // can act on, so it is reported as the relay being unavailable.
              Effect.catchTag("PlatformError", () => new PeerRpcError.ServerUnavailable())
            )
            const inboxKeySelf = yield* encodeInboxKey(principal).pipe(
              Effect.provideService(Crypto.Crypto, crypto),
              Effect.catchTag("ReplicaError", () => new PeerRpcError.ServerUnavailable())
            )
            const inboxKeyRemote = yield* encodeInboxKey(send.remote).pipe(
              Effect.provideService(Crypto.Crypto, crypto),
              Effect.catchTag("ReplicaError", () => new PeerRpcError.ServerUnavailable())
            )

            const session: Session = {
              sessionId,
              principal,
              remote: send.remote,
              documents: payload.documents,
              senderReplicaIncarnation: payload.senderReplicaIncarnation,
              senderRetryHorizonMillis: payload.senderRetryHorizonMillis,
              inboxKeySelf,
              inboxKeyRemote
            }

            sessions.set(sessionId, session)
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                sessions.delete(sessionId)
              }).pipe(
                // Best effort: the entity's own liveness deadline is what actually bounds an
                // abandoned session, because this never runs when the node itself fails.
                Effect.andThen(Effect.ignore(inboxClient(inboxKeySelf).EndSession({ sessionId })))
              )
            )

            // A session must not outlive what authorized it. The credential and both grants each
            // expose an `invalidated` signal and a deadline, and none of them is observed by the
            // client, so the relay is the only place that can end the session when authority is
            // withdrawn. Without this the heartbeat below would keep an unauthorized session alive
            // for as long as the socket stays open.
            const authorityLapsesAt = Math.min(
              authenticated.validUntil,
              send.validUntil,
              receive.validUntil
            )
            yield* Effect.raceAll([
              authenticated.invalidated,
              send.invalidated,
              receive.invalidated,
              Effect.sleep(Math.max(0, authorityLapsesAt - authorizedAt))
            ]).pipe(
              Effect.andThen(
                Effect.logInfo("Relay session authority withdrawn; ending session").pipe(
                  Effect.annotateLogs({ sessionId })
                )
              ),
              Effect.andThen(Effect.ignore(inboxClient(inboxKeySelf).EndSession({ sessionId }))),
              Effect.forkScoped
            )

            // Proves this node and its socket are still alive. Stopping is the signal.
            yield* Effect.ignore(inboxClient(inboxKeySelf).Heartbeat({ sessionId })).pipe(
              Effect.repeat(Schedule.spaced(options.heartbeatIntervalMillis)),
              Effect.forkScoped
            )

            const opened: PeerRpc.OpenEvent = {
              _tag: "Opened",
              protocolVersion: PeerRpc.protocolVersion,
              sessionId,
              remotePeerId: send.remote.peerId,
              authenticatedLocal: principal
            }

            // `Stream.concat` builds the second stream only after the first completes, so the
            // subscription attaches after `Opened` reaches the wire. A client that sees `Opened` has
            // a session the relay accepted, not yet one the owning entity has attached.
            return Stream.concat(
              Stream.succeed(opened),
              inboxClient(inboxKeySelf).Subscribe({ sessionId }).pipe(
                // Re-authorized per message rather than once at handshake. A grant can be narrowed
                // or revoked mid session, and anything with a Send grant to this device can write
                // into its inbox, so the handshake's Receive decision cannot stand in for the
                // recipient's right to see a particular document. A message that fails here is left
                // unsettled and simply not emitted, so it stays durable for a later session.
                Stream.filterEffect((message) =>
                  authorization.authorize({
                    direction: "Receive",
                    principal: session.principal,
                    remote: session.remote,
                    documents: [{
                      documentType: message.document.documentType,
                      documentId: message.document.documentId
                    }]
                  }).pipe(
                    // Delivering commits this peer to decoding the document, so the same risk
                    // acknowledgement the sender needed is required again on the receiving side, per
                    // message rather than once at the handshake.
                    Effect.andThen(authorization.authorizeUnsafeUnboundedAutomerge3Decode({
                      risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
                      direction: "Receive",
                      principal: session.principal,
                      remote: session.remote,
                      documents: [{
                        documentType: message.document.documentType,
                        documentId: message.document.documentId
                      }]
                    })),
                    Effect.as(true),
                    Effect.catchTag("AccessDenied", () =>
                      Effect.logInfo("Withheld a delivery the recipient may no longer receive").pipe(
                        Effect.annotateLogs({
                          sessionId,
                          documentId: message.document.documentId
                        }),
                        // The message has already left the entity's queue, so its delivering fiber
                        // is waiting for a settlement that can never arrive and is holding its
                        // channel while it waits. Releasing ends that attempt without settling: the
                        // row stays durable for a later session, and the channel stops occupying
                        // one of this session's delivery slots. Without this one withheld message
                        // stalls its channel for the whole session, and enough of them starve
                        // channels the recipient is perfectly entitled to receive.
                        Effect.andThen(release(inboxKeySelf, sessionId, message)),
                        Effect.as(false)
                      )),
                    Effect.catchTag("ServerUnavailable", () =>
                      release(inboxKeySelf, sessionId, message).pipe(Effect.as(false)))
                  )
                ),
                // Cluster level failures are not part of the public contract, so each is reported as
                // the wire error that tells the client what to do about it.
                Stream.catchTag("MailboxFull", () =>
                  Stream.fail(new PeerRpcError.SessionOverloaded())),
                Stream.catchTag(
                  "AlreadyProcessingMessage",
                  () => Stream.fail(new PeerRpcError.SessionOverloaded())
                ),
                Stream.catchTag(
                  "PersistenceError",
                  () => Stream.fail(new PeerRpcError.ServerUnavailable())
                )
              )
            )
          }))
        ),

      Push: (payload) =>
        Effect.gen(function*() {
          const authenticated = yield* PeerAuthentication.AuthenticatedPeer
          const session = yield* sessionFor(payload.sessionId, authenticated.principal)

          // The recipient is fixed by the handshake, never taken from the request, so a session can
          // only ever send to the counterparty it was authorized for.
          const envelope = yield* decodeEnvelope(new TextDecoder().decode(payload.payload)).pipe(
            Effect.catchTag("SchemaError", () => new PeerRpcError.InvalidRequest())
          )
          const document = session.documents.find((candidate) =>
            candidate.documentId === envelope.documentId &&
            candidate.documentType === envelope.documentType
          )
          if (document === undefined) {
            return yield* new PeerRpcError.InvalidRequest()
          }
          // The sender's own hash is recomputed rather than believed: it is the identity the
          // recipient will deduplicate on, so an unchecked one lets a sender mislabel its content.
          const messageHash = yield* Canonical.digest(envelope.message).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.catchTag("ReplicaError", () => new PeerRpcError.ServerUnavailable())
          )
          if (messageHash !== envelope.messageHash) {
            return yield* new PeerRpcError.InvalidRequest()
          }

          const send = yield* authorization.authorize({
            direction: "Send",
            principal: session.principal,
            remote: session.remote,
            documents: [document]
          })
          if (send.documents.length === 0) {
            return yield* new PeerRpcError.AccessDenied()
          }
          // Relaying a document commits its recipient to decoding it, and that decode is not
          // allocation bounded on the Automerge version this protocol targets. The deployment has
          // to acknowledge that risk explicitly for this document rather than inherit it from the
          // ordinary Send grant, which is why it is a separate port that denies by default.
          yield* authorization.authorizeUnsafeUnboundedAutomerge3Decode({
            risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
            direction: "Send",
            principal: session.principal,
            remote: session.remote,
            documents: [document]
          })

          const outerEnvelopeDigest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope({
            domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
            version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
            expectedLocal: session.principal,
            remote: session.remote,
            relayPeerId: options.peerId,
            relayMessageId: payload.relayMessageId,
            protocolVersion: PeerRpc.protocolVersion,
            payloadVersion: 1,
            senderReplicaIncarnation: session.senderReplicaIncarnation,
            senderConnectionEpoch: envelope.connectionEpoch,
            senderSequence: envelope.sequence,
            document: {
              documentId: envelope.documentId,
              documentType: envelope.documentType
            },
            lineage: envelope.lineage,
            writerProvenance: envelope.writerProvenance,
            messageHash: envelope.messageHash,
            payload: payload.payload
          }).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            // A client can produce an envelope that decodes yet fails to canonicalize, so this is
            // a permanent rejection. Reporting it as unavailability would have the sender's outbox
            // retry it forever, paying a full canonical encode on the relay each time.
            Effect.catchReason("ReplicaError", "ProtocolMismatch", () => new PeerRpcError.InvalidRequest()),
            Effect.catchTag("ReplicaError", () => new PeerRpcError.ServerUnavailable())
          )

          const sender = {
            tenantId: session.principal.tenantId,
            subjectId: session.principal.subjectId,
            peerId: session.principal.peerId,
            replicaIncarnation: session.senderReplicaIncarnation,
            connectionEpoch: envelope.connectionEpoch,
            sequence: envelope.sequence
          }

          yield* bounded(
            inboxClient(session.inboxKeyRemote).Deliver({
              channel: {
                tenantId: session.principal.tenantId,
                senderSubjectId: session.principal.subjectId,
                senderPeerId: session.principal.peerId,
                senderReplicaIncarnation: session.senderReplicaIncarnation,
                senderConnectionEpoch: envelope.connectionEpoch
              },
              envelope: {
                relayMessageId: payload.relayMessageId,
                relayPeerId: options.peerId,
                sender,
                recipient: session.remote,
                payloadVersion: 1,
                document: {
                  documentId: envelope.documentId,
                  documentType: envelope.documentType
                },
                writerProvenance: envelope.writerProvenance,
                messageHash: envelope.messageHash,
                outerEnvelopeDigest,
                payload: payload.payload
              },
              senderRetryHorizonMillis: session.senderRetryHorizonMillis
            })
          ).pipe(
            Effect.catchTag("MailboxFull", () => new PeerRpcError.SessionOverloaded()),
            Effect.catchTag("AlreadyProcessingMessage", () => new PeerRpcError.SessionOverloaded()),
            Effect.catchTag("PersistenceError", () => new PeerRpcError.ServerUnavailable())
          )
        }).pipe(
          Effect.withSpan("RelayServer.Push", {
            attributes: { relay_message_id: payload.relayMessageId }
          })
        ),

      Acknowledge: (payload) => settle(payload, "Acknowledged"),

      Reject: (payload) => settle(payload, "Rejected")
    })
  }))

/**
 * The relay server: the front door plus the entity behaviour it forwards to.
 *
 * Requires a `Sharding` but never builds one, so the deployment shape stays the consumer's choice —
 * an in-memory single process for tests, one runner over SQL, or many runners sharded across
 * machines — without the relay changing.
 */
export const layer = (
  options: Options & {
    readonly inbox: RelayInbox.Options
    /**
     * Retention for every inbox in the deployment.
     *
     * Required rather than optional, and composed here rather than left to the consumer, because
     * `messageTtlMillis` and `terminalRetentionMillis` are inert without it: a deployment that
     * forgot to add the singleton separately would expire nothing and collect nothing, and would
     * only find out when the table had grown past the point where admission still succeeded.
     */
    readonly maintenance: RelayInboxMaintenance.Options
  }
) =>
  Layer.mergeAll(
    layerHandlers(options),
    RelayInbox.layer(options.inbox),
    RelayInboxMaintenance.layer(options.maintenance)
  ).pipe(
    // Two independently configured values have to agree or every session is reaped on a fixed
    // cycle and no client can hold a delivery stream — a total outage that only appears under a
    // particular configuration, so it is refused at construction rather than discovered in
    // production. The factor of two leaves room for one lost heartbeat.
    Layer.provide(Layer.effectDiscard(
      options.heartbeatIntervalMillis * 2 < options.inbox.sessionDeadlineMillis &&
        options.entityCallTimeoutMillis > 0
        ? Effect.void
        : Effect.die(
          new Error(
            "RelayServer: heartbeatIntervalMillis * 2 must be less than " +
              "inbox.sessionDeadlineMillis, and entityCallTimeoutMillis must be positive"
          )
        )
    ))
  )
