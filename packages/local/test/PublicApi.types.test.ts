import type * as Automerge from "@automerge/automerge"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import {
  Document,
  DocumentSet,
  Identity,
  Mutation,
  PeerTransport,
  Query,
  SchemaDescriptor,
  Transient
} from "../src/index.js"
import type { CommandOutcome, Conflict, Replica } from "../src/index.js"
import type * as ReplicaError from "../src/ReplicaError.js"

type Equal<A, B,> = (<T,>() => T extends A ? 1 : 2) extends <T,>() => T extends B ? 1 : 2 ? true : false

type ExpectedInspectionError =
  | Conflict.UnsupportedConflictValue
  | Conflict.UnsupportedConflictKey

type ExpectedResolutionError =
  | ExpectedInspectionError
  | Conflict.StaleConflictResolution
  | Conflict.ConflictPathNotFound
  | Conflict.ConflictPathTypeMismatch
  | Conflict.ConflictNotFound
  | Conflict.ConflictAlternativeNotFound
  | Conflict.CompositeAlternativeRequiresReplacement
  | Conflict.ConflictResolutionSchemaError

type ExpectedPath = {
  readonly parents: ReadonlyArray<
    | {
      readonly _tag: "Key"
      readonly key: string
      readonly alternative?: Conflict.AlternativeId
    }
    | {
      readonly _tag: "Index"
      readonly index: number
      readonly alternative?: Conflict.AlternativeId
    }
  >
  readonly target:
    | { readonly _tag: "Key"; readonly key: string }
    | { readonly _tag: "Index"; readonly index: number }
}

type ExpectedChoice =
  | {
    readonly _tag: "SelectAlternative"
    readonly alternativeId: Conflict.AlternativeId
  }
  | {
    readonly _tag: "ReplaceValue"
    readonly value: Automerge.AutomergeValue
  }
  | { readonly _tag: "DeleteValue" }

type ExpectedResolution = {
  readonly heads: ReadonlyArray<string>
  readonly path: ExpectedPath
  readonly choice: ExpectedChoice
}

describe("public API types", () => {
  class ReadError extends Schema.TaggedErrorClass<ReadError>()("ReadError", {}) {}
  class RenameError extends Schema.TaggedErrorClass<RenameError>()("RenameError", {}) {}
  const DomainError = Schema.Union([ReadError, RenameError])

  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const Note = Document.make("Note", {
    schema: Schema.Struct({ body: Schema.String }),
    version: 1
  })
  const Rename = Mutation.make("Rename", {
    document: Task,
    payload: { title: Schema.String },
    success: Schema.Boolean,
    error: RenameError
  })
  const Read = Query.make("Read", {
    payload: {
      id: Schema.NumberFromString,
      label: Schema.optionalKey(Schema.String)
    },
    success: Schema.Array(Task.schema),
    error: ReadError,
    dependsOn: []
  })
  const RenameWithUnion = Mutation.make("RenameWithUnion", {
    document: Task,
    payload: Schema.Struct({ title: Schema.String }),
    error: DomainError
  })
  class Typing extends Schema.TaggedClass<Typing>()("Typing", { userId: Schema.String }) {}
  class ReadPosition extends Schema.TaggedClass<ReadPosition>()("ReadPosition", { messageId: Schema.String }) {}
  const ChatTransient = Transient.make("ChatActivity", {
    document: Task,
    payload: Schema.Union([Typing, ReadPosition])
  })
  const ReadWithUnion = Query.make("ReadWithUnion", {
    success: Schema.Array(Task.schema),
    error: DomainError,
    dependsOn: []
  })

  it("preserves document, mutation, and query inference", () => {
    const documentDecoded: Equal<typeof Task.schema.Type, { readonly title: string }> = true
    const mutationPayload: Equal<typeof Rename.payloadSchema.Type, { readonly title: string }> = true
    const mutationSuccess: Equal<typeof Rename.successSchema.Type, boolean> = true
    const mutationError: Equal<typeof Rename.errorSchema.Type, RenameError> = true
    const queryPayload: Equal<
      typeof Read.payloadSchema.Type,
      { readonly id: number; readonly label?: string }
    > = true
    const queryPayloadEncoded: Equal<
      typeof Read.payloadSchema.Encoded,
      { readonly id: string; readonly label?: string }
    > = true
    const querySuccess: Equal<typeof Read.successSchema.Type, ReadonlyArray<{ readonly title: string }>> = true
    const queryError: Equal<typeof Read.errorSchema.Type, ReadError> = true
    const mutationErrorUnion: Equal<typeof RenameWithUnion.errorSchema.Type, ReadError | RenameError> = true
    const queryErrorUnion: Equal<typeof ReadWithUnion.errorSchema.Type, ReadError | RenameError> = true
    assert.isTrue(documentDecoded)
    assert.isTrue(mutationPayload)
    assert.isTrue(mutationSuccess)
    assert.isTrue(mutationError)
    assert.isTrue(queryPayload)
    assert.isTrue(queryPayloadEncoded)
    assert.isTrue(querySuccess)
    assert.isTrue(queryError)
    assert.isTrue(mutationErrorUnion)
    assert.isTrue(queryErrorUnion)
  })

  it("preserves transient tagged union inference", () => {
    const payload: Equal<typeof ChatTransient.payloadSchema.Type, Typing | ReadPosition> = true
    const client: Equal<
      typeof ChatTransient.client,
      Effect.Effect<
        (documentId: Identity.DocumentId) => Transient.Client<Typing | ReadPosition>,
        never,
        Transient.Transient
      >
    > = true
    assert.isTrue(payload)
    assert.isTrue(client)
  })

  it("narrows document lookup by literal name", () => {
    const documents = DocumentSet.make(Task, Note)
    const task = DocumentSet.get(documents, "Task")
    const name: string = "Task"
    const dynamic = DocumentSet.get(documents, name)
    const literalLookup: Equal<typeof task, typeof Task | undefined> = true
    const dynamicLookup: Equal<typeof dynamic, typeof Task | typeof Note | undefined> = true
    assert.isTrue(literalLookup)
    assert.isTrue(dynamicLookup)
  })

  it("exports the schema descriptor contract", () => {
    const descriptor: SchemaDescriptor.Descriptor = SchemaDescriptor.make(Schema.String)
    assert.isDefined(descriptor)
  })

  it("preserves conflict schemas and replica operation inference", () => {
    const nativeValue: Equal<Conflict.Value, Automerge.AutomergeValue> = true
    const encodedValue: Equal<Conflict.EncodedValue, Conflict.PortableValue> = true
    const resolution: Equal<Conflict.Resolution, ExpectedResolution> = true
    const path: Equal<Conflict.Path, ExpectedPath> = true
    const choice: Equal<Conflict.Choice, ExpectedChoice> = true
    const inspectionError: Equal<Conflict.InspectionError, ExpectedInspectionError> = true
    const resolutionError: Equal<Conflict.ResolutionError, ExpectedResolutionError> = true

    const assertReplicaTypes = (
      replica: Replica.Replica["Service"],
      documentId: Identity.DocumentId,
      commandId: Identity.CommandId,
      conflictResolution: Conflict.Resolution
    ): void => {
      const inspected = replica.inspectConflicts(Task, documentId)
      const resolved = replica.resolveConflict(Task, {
        commandId,
        documentId,
        resolution: conflictResolution
      })
      const lookedUp = replica.lookupConflictResolution(Task, {
        commandId,
        documentId,
        resolution: conflictResolution
      })
      const inspectSuccess: Equal<
        Effect.Success<typeof inspected>,
        Conflict.Inspection<{ readonly title: string }>
      > = true
      const inspectError: Equal<
        Effect.Error<typeof inspected>,
        ExpectedInspectionError | ReplicaError.ReplicaError
      > = true
      const resolveSuccess: Equal<Effect.Success<typeof resolved>, void> = true
      const resolveError: Equal<
        Effect.Error<typeof resolved>,
        ExpectedResolutionError | ReplicaError.ReplicaError
      > = true
      const lookupSuccess: Equal<
        Effect.Success<typeof lookedUp>,
        CommandOutcome.CommandOutcome<void, ExpectedResolutionError>
      > = true
      const lookupError: Equal<Effect.Error<typeof lookedUp>, ReplicaError.ReplicaError> = true
      void [
        inspectSuccess,
        inspectError,
        resolveSuccess,
        resolveError,
        lookupSuccess,
        lookupError
      ]
    }
    void assertReplicaTypes

    assert.isTrue(nativeValue)
    assert.isTrue(encodedValue)
    assert.isTrue(resolution)
    assert.isTrue(path)
    assert.isTrue(choice)
    assert.isTrue(inspectionError)
    assert.isTrue(resolutionError)
  })

  it("requires acknowledged durable relay delivery", () => {
    const peerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
    const relayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000003")
    const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000004")
    const identity: PeerTransport.RelayDeliveryIdentity = {
      relayMessageId,
      relayPeerId: peerId,
      senderTenantId: "tenant",
      senderSubjectId: "subject",
      senderPeerId: peerId,
      senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
      messageHash: "a".repeat(64),
      outerEnvelopeDigest: "b".repeat(64)
    }
    const delivery: PeerTransport.AcknowledgedDelivery = {
      message: new Uint8Array([1]),
      identity,
      receiptRetentionMillis: 1,
      acknowledge: Effect.void,
      reject: (_reason) => Effect.void
    }
    const inbound: PeerTransport.Inbound = { _tag: "Durable", delivery }
    const transientDelivery: PeerTransport.TransientDelivery = {
      peerId,
      documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000005"),
      payload: new Uint8Array([2])
    }
    const transientInbound: PeerTransport.Inbound = { _tag: "Transient", delivery: transientDelivery }
    const connection = {
      peerId,
      relayPeerId,
      capabilities: {},
      receive: Stream.make(inbound, transientInbound),
      send: () => Effect.void,
      transient: () => Effect.void,
      close: Effect.void
    } satisfies PeerTransport.Connection
    const acknowledgedReceive: Equal<
      PeerTransport.Connection["receive"],
      Stream.Stream<PeerTransport.Inbound, ReplicaError.ReplicaError>
    > = true
    const requiredRelayPeerId: Equal<
      PeerTransport.Connection["relayPeerId"],
      Identity.PeerId
    > = true
    const transport = PeerTransport.PeerTransport.of({
      capabilities: {},
      connect: () => Effect.succeed(connection)
    })
    const protocolRejection = delivery.reject("ProtocolInvalid")
    const applicationRejection = delivery.reject("ApplicationRejected")
    assert.isDefined(transport)
    assert.strictEqual(connection.relayPeerId, relayPeerId)
    assert.isDefined(connection.receive)
    assert.isTrue(acknowledgedReceive)
    assert.isTrue(requiredRelayPeerId)
    assert.isDefined(protocolRejection)
    assert.isDefined(applicationRejection)
  })
})
