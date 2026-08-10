import * as CommandDeliveryPublisher from "@lucas-barake/effect-local-sql/CommandDeliveryPublisher"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Backup from "@lucas-barake/effect-local/Backup"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import type * as Document from "@lucas-barake/effect-local/Document"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { RpcServer } from "effect/unstable/rpc"
import * as RestoreTransport from "./internal/RestoreTransport.js"
import * as Wire from "./internal/wire.js"
import * as ReplicaRpc from "./ReplicaRpc.js"
import * as SessionManager from "./SessionManager.js"

const lookup = <A,>(
  values: ReadonlyMap<string, A>,
  kind: string,
  name: string
): Effect.Effect<A, ReplicaError.ReplicaError> => {
  const value = values.get(name)
  if (value === undefined) {
    return Effect.fail(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: `known ${kind}`,
          observed: name
        })
      })
    )
  }
  return Effect.succeed(value)
}

export const layerHandlers = (replicaDefinition: ReplicaDefinition.Any) =>
  ReplicaRpc.group.toLayer(Effect.gen(function*() {
    const replica = yield* Replica.Replica
    const sessions = yield* SessionManager.SessionManager
    const restoreTransport = yield* RestoreTransport.RestoreTransport
    const commits = yield* CommitPublisher.CommitPublisher
    const peerConnections = yield* PeerConnectionStatus.PeerConnectionStatus
    const peerRelay = yield* Effect.serviceOption(PeerRelayClientRuntime.PeerRelayClientRuntime)
    const relayConnection = yield* RelayConnectionStatus.RelayConnectionStatus
    const deliveries = yield* CommandDeliveryPublisher.CommandDeliveryPublisher
    const crypto = yield* Crypto.Crypto
    const ownerEpoch = yield* crypto.randomUUIDv4.pipe(
      Effect.mapError((cause) =>
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.StorageUnavailable({
            cause
          })
        })
      )
    )
    const documents = new Map<string, Document.Any>(
      replicaDefinition.documents.documents.map((document: Document.Any) => [document.name, document])
    )
    const mutations = new Map<string, Mutation.Any>(
      replicaDefinition.mutations.map((mutation: Mutation.Any) => [mutation.name, mutation])
    )
    const queries = new Map<string, Query.Any>(
      replicaDefinition.queries.map((query: Query.Any) => [query.name, query])
    )
    const allInvalidationKeys = ReplicaDefinition.invalidationKeys(replicaDefinition)
    return ReplicaRpc.group.of({
      OpenSession: ({ definitionHash, protocolVersion, sessionId }, { client }) => {
        if (protocolVersion === ReplicaRpc.protocolVersion && definitionHash === replicaDefinition.hash) {
          return sessions.open(sessionId, client.id).pipe(Effect.as({
            leaseMillis: SessionManager.leaseDurationMillis,
            protocolVersion: ReplicaRpc.protocolVersion,
            definitionHash: replicaDefinition.hash,
            ownerEpoch,
            conflictLimits: sessions.conflictLimits,
            maxChunkBytes: sessions.maxChunkBytes,
            maxRestoreCoalesceMillis: sessions.maxRestoreCoalesceMillis,
            maxRestoreErrorBytes: sessions.maxRestoreErrorBytes
          }))
        }
        return Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: `${ReplicaRpc.protocolVersion}:${replicaDefinition.hash}`,
              observed: `${protocolVersion ?? "missing"}:${definitionHash}`
            })
          })
        )
      },
      RenewSession: ({ sessionId }, { client }) =>
        sessions.renew(sessionId, client.id).pipe(Effect.as({ leaseMillis: SessionManager.leaseDurationMillis })),
      CloseSession: ({ sessionId }, { client }) => sessions.close(sessionId, client.id),
      Create: ({ commandId, document, sessionId, value }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              Wire.decode(definition.schema, value).pipe(
                Effect.flatMap((decoded) =>
                  CommandOutcome.toOutcome(commandId, replica.create(definition, { commandId, value: decoded }))
                )
              )
            )
          )
        ),
      Get: ({ document, documentId, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              replica.get(definition, documentId).pipe(
                Effect.flatMap((snapshot) =>
                  Wire.encode(definition.schema, snapshot.value).pipe(
                    Effect.map((value) => ({ ...snapshot, value }))
                  )
                )
              )
            )
          )
        ),
      InspectConflicts: ({ document, documentId, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              replica.inspectConflicts(definition, documentId).pipe(
                Effect.flatMap((inspection) =>
                  Wire.encode(definition.schema, inspection.snapshot.value).pipe(
                    Effect.flatMap((value) => {
                      const encoded = {
                        ...inspection,
                        snapshot: { ...inspection.snapshot, value }
                      }
                      return Wire.encodeConflict(
                        Conflict.inspection(Schema.Json),
                        encoded,
                        sessions.conflictLimits,
                        Conflict.preflightInspection
                      )
                    })
                  )
                ),
                Effect.withSpan("ReplicaOwner.inspectConflicts", {
                  attributes: { "document.type": definition.name }
                })
              )
            )
          )
        ),
      ResolveConflict: ({ commandId, document, documentId, resolution, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              Wire.decodeConflict(
                Conflict.Resolution,
                resolution,
                sessions.conflictLimits,
                Conflict.preflightResolution
              ).pipe(
                Effect.flatMap((decoded) =>
                  CommandOutcome.toOutcome(
                    commandId,
                    replica.resolveConflict(definition, {
                      commandId,
                      documentId,
                      resolution: decoded
                    })
                  ).pipe(
                    Effect.flatMap((outcome) =>
                      Wire.encodeConflict(
                        CommandOutcome.schema(Schema.Void, Conflict.ResolutionError),
                        outcome,
                        sessions.conflictLimits
                      )
                    ),
                    Effect.withSpan("ReplicaOwner.resolveConflict", {
                      attributes: {
                        "document.type": definition.name,
                        "conflict.choice": decoded.choice._tag,
                        "conflict.path_depth": decoded.path.parents.length + 1
                      }
                    })
                  )
                )
              )
            )
          )
        ),
      Mutate: ({ commandId, documentId, mutation, payload, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(mutations, "mutation", mutation).pipe(
            Effect.flatMap((definition) =>
              Wire.decode(definition.payloadSchema, payload).pipe(
                Effect.flatMap((decoded) =>
                  CommandOutcome.toOutcome(
                    commandId,
                    replica.mutate(definition, { commandId, documentId, payload: decoded })
                  )
                ),
                Effect.flatMap((outcome) =>
                  Wire.encodeOutcome(definition.successSchema, definition.errorSchema, outcome)
                )
              )
            )
          )
        ),
      Delete: ({ commandId, document, documentId, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              CommandOutcome.toOutcome(commandId, replica.delete(definition, { commandId, documentId })).pipe(
                Effect.flatMap((outcome) => Wire.encodeOutcome(Schema.Void, Schema.Never, outcome))
              )
            )
          )
        ),
      Query: ({ payload, query, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(queries, "query", query).pipe(
            Effect.flatMap((definition) =>
              Wire.decode(definition.payloadSchema, payload).pipe(
                Effect.flatMap((decoded) => replica.query(definition, decoded)),
                Effect.matchEffect({
                  onSuccess: (result) => Wire.encode(definition.successSchema, result),
                  onFailure: (error) => {
                    if (Schema.is(ReplicaError.ReplicaError)(error)) return Effect.fail(error)
                    return Wire.encode(definition.errorSchema, error).pipe(
                      Effect.flatMap((encoded) => Effect.fail(new ReplicaRpc.ReplicaQueryError({ error: encoded })))
                    )
                  }
                })
              )
            )
          )
        ),
      LookupMutation: ({ commandId, mutation, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(mutations, "mutation", mutation).pipe(
            Effect.flatMap((definition) =>
              replica.lookupMutation(definition, commandId).pipe(
                Effect.flatMap((outcome) =>
                  Wire.encodeOutcome(definition.successSchema, definition.errorSchema, outcome)
                )
              )
            )
          )
        ),
      LookupCreate: ({ commandId, document, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) => replica.lookupCreate(definition, commandId))
          )
        ),
      LookupDelete: ({ commandId, document, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              replica.lookupDelete(definition, commandId).pipe(
                Effect.flatMap((outcome) => Wire.encodeOutcome(Schema.Void, Schema.Never, outcome))
              )
            )
          )
        ),
      LookupConflictResolution: (
        { commandId, document, documentId, resolution, sessionId },
        { client }
      ) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              Wire.decodeConflict(
                Conflict.Resolution,
                resolution,
                sessions.conflictLimits,
                Conflict.preflightResolution
              ).pipe(
                Effect.flatMap((decoded) =>
                  replica.lookupConflictResolution(definition, {
                    commandId,
                    documentId,
                    resolution: decoded
                  }).pipe(
                    Effect.flatMap((outcome) =>
                      Wire.encodeConflict(
                        CommandOutcome.schema(Schema.Void, Conflict.ResolutionError),
                        outcome,
                        sessions.conflictLimits
                      )
                    ),
                    Effect.withSpan("ReplicaOwner.lookupConflictResolution", {
                      attributes: {
                        "document.type": definition.name,
                        "conflict.choice": decoded.choice._tag,
                        "conflict.path_depth": decoded.path.parents.length + 1
                      }
                    })
                  )
                )
              )
            )
          )
        ),
      LookupCommandDelivery: ({ commandId, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          replica.lookupCommandDelivery(commandId)
        ),
      CommandDeliveryChanges: ({ commandId, sessionId }, { client }) =>
        sessions.stream(
          sessionId,
          client.id,
          replica.commandDeliveryChanges(commandId)
        ),
      Flush: ({ sessionId }, { client }) => sessions.run(sessionId, client.id, replica.flush),
      Invalidations: ({ ownerEpoch: requestedEpoch, sessionId }, { client }) => {
        if (requestedEpoch === ownerEpoch) {
          return sessions.stream(
            sessionId,
            client.id,
            Effect.all([commits.subscribe, deliveries.subscribe]).pipe(
              Effect.map(([commitSubscription, deliverySubscription]) =>
                Stream.make({
                  _tag: "InvalidationsReady",
                  ownerEpoch,
                  watermark: commitSubscription.watermark,
                  refreshGeneration: commitSubscription.refreshGeneration,
                  deliveryWatermark: deliverySubscription.sequence,
                  deliveryRefreshEpoch: deliverySubscription.refreshEpoch
                }).pipe(
                  Stream.concat(
                    Stream.merge(
                      commitSubscription.events.pipe(
                        Stream.map((event): ReplicaRpc.InvalidationMessage => {
                          if (event._tag === "Commit") {
                            return {
                              _tag: "Invalidation",
                              ownerEpoch,
                              sequence: event.commitSequence,
                              keys: event.keys
                            }
                          }
                          return { _tag: "FullRefreshRequired", ownerEpoch, keys: allInvalidationKeys }
                        })
                      ),
                      deliverySubscription.events.pipe(
                        Stream.map((event): ReplicaRpc.InvalidationMessage => {
                          if (event._tag === "Delivery") {
                            return {
                              _tag: "DeliveryInvalidation",
                              ownerEpoch,
                              sequence: event.sequence,
                              keys: [ReplicaRpc.commandDeliveryInvalidationKey]
                            }
                          }
                          return {
                            _tag: "DeliveryFullRefreshRequired",
                            ownerEpoch,
                            keys: [ReplicaRpc.commandDeliveryInvalidationKey]
                          }
                        })
                      )
                    )
                  )
                )
              ),
              Stream.unwrap,
              Stream.scoped
            )
          )
        }
        return Stream.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: ownerEpoch,
              observed: requestedEpoch
            })
          })
        )
      },
      Status: ({ sessionId }, { client }) => sessions.stream(sessionId, client.id, replica.status),
      PeerConnectionStatus: ({ peerId, sessionId }, { client }) =>
        sessions.stream(
          sessionId,
          client.id,
          peerConnections.status(peerId).pipe(Stream.withSpan("ReplicaOwner.peerConnectionStatus"))
        ),
      RelayConnectionStatus: ({ sessionId }, { client }) =>
        sessions.stream(
          sessionId,
          client.id,
          relayConnection.status.pipe(Stream.withSpan("ReplicaOwner.relayConnectionStatus"))
        ),
      Transient: ({ documentId, payload, peerId, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          Option.match(peerRelay, {
            onNone: () =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: "relay configured transient messaging",
                    observed: "direct replica"
                  })
                })
              ),
            onSome: (runtime) => runtime.send(peerId, documentId, payload)
          }).pipe(
            Effect.withSpan("ReplicaOwner.transient", {
              attributes: { "peer.id": peerId, "document.id": documentId }
            })
          )
        ),
      Transients: ({ sessionId }, { client }) =>
        sessions.stream(
          sessionId,
          client.id,
          Option.match(peerRelay, {
            onNone: () =>
              Stream.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: "relay configured transient messaging",
                    observed: "direct replica"
                  })
                })
              ),
            onSome: (runtime) => runtime.transients
          }).pipe(Stream.withSpan("ReplicaOwner.transients"))
        ),
      ExportBackup: ({ maxBytes, sessionId }, { client }) =>
        sessions.stream(
          sessionId,
          client.id,
          replica.exportBackup({ maxBytes }).pipe(
            Stream.map((chunk) => new Uint8Array(chunk))
          )
        ),
      RestoreBackup: ({ sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "BeginRestoreBackup",
                observed: "RestoreBackup"
              })
            })
          )
        ),
      BeginRestoreBackup: (
        options,
        { client }
      ) =>
        Backup.validateMaxBytes(options.maxBytes).pipe(
          Effect.flatMap((validatedMaxBytes) => {
            if (options.mode === "document") {
              return lookup(documents, "document", options.document).pipe(
                Effect.flatMap((document) =>
                  restoreTransport.begin({
                    sessionId: options.sessionId,
                    clientId: client.id,
                    mode: options.mode,
                    document,
                    documentId: options.documentId,
                    maxBytes: validatedMaxBytes,
                    expectedDefinitionHash: options.expectedDefinitionHash,
                    installationId: options.installationId
                  })
                )
              )
            }
            return restoreTransport.begin({
              sessionId: options.sessionId,
              clientId: client.id,
              mode: options.mode,
              maxBytes: validatedMaxBytes,
              expectedDefinitionHash: options.expectedDefinitionHash,
              installationId: options.installationId
            })
          })
        ),
      FinishRestoreBackup: ({ nonce, sessionId }, { client }) =>
        restoreTransport.finish({
          sessionId,
          clientId: client.id,
          nonce
        }),
      ExportDocument: ({ document, documentId, sessionId }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              replica.exportDocument(definition, documentId).pipe(
                Effect.flatMap((exported) =>
                  Wire.encode(Schema.toEncoded(definition.schema), exported.value).pipe(
                    Effect.map((value) => ({ ...exported, value }))
                  )
                )
              )
            )
          )
        ),
      ImportDocument: ({ commandId, document, sessionId, value }, { client }) =>
        sessions.run(
          sessionId,
          client.id,
          lookup(documents, "document", document).pipe(
            Effect.flatMap((definition) =>
              Wire.decode(Schema.toEncoded(definition.schema), value.value).pipe(
                Effect.flatMap((decoded) =>
                  CommandOutcome.toOutcome(
                    commandId,
                    replica.importDocument(definition, {
                      commandId,
                      value: { ...value, value: decoded }
                    })
                  )
                )
              )
            )
          )
        )
    })
  })).pipe(Layer.provide(RestoreTransport.freshLayer))

export const layer = (definition: ReplicaDefinition.Any) =>
  // One request's defect must answer that request, not tear down the tab's whole session.
  RpcServer.layer(ReplicaRpc.group, { disableFatalDefects: true }).pipe(
    Layer.provide(layerHandlers(definition))
  )

export const layerWorker = (definition: ReplicaDefinition.Any) =>
  layer(definition).pipe(Layer.provide(RpcServer.layerProtocolWorkerRunner))
