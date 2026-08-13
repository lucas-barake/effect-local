import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as AttachmentClient from "../src/AttachmentClient.js"
import * as FileSystemAttachmentStorage from "../src/FileSystemAttachmentStorage.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"

const Message = Model.make("Message", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({
    id: Schema.String,
    body: Schema.String,
    attachment: Attachment.Reference
  })
})
const PutMessage = Mutation.make("PutMessage", { version: 1, payload: Message.schema })
const definition = Definition.make({ version: 1, models: [Message], mutations: [PutMessage] })
const layerHandlers = PutMessage.toLayer(({ payload, transaction }) => transaction.set(Message, payload.id, payload))
const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const hello = Uint8Array.from([104, 101, 108, 108, 111])
const layerNodeServices = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
const provideNodeServices = Effect.provide(layerNodeServices)
const layerRemote = Layer.succeed(
  SyncEngine.SyncEngine,
  SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
    discard: () => Effect.die("unexpected discard"),
    pull: () => Effect.never,
    bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
    watch: () => Stream.never
  })
)
const replicaOptions = {
  definition,
  clientId,
  initialSpaces: [spaceId],
  scope: Protocol.ReplicationScope.make({ models: [Message.name] }),
  retainedReceipts: 256,
  settlementCapacity: 64,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
} satisfies SqlReplica.Options<typeof definition>

const collectBytes = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

describe("replica attachments", () => {
  it.effect(
    "commits an offline reference without putting bytes on the JSON wire",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-replica-attachments-" })
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/objects`,
          maximumBytes: 8
        })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerAttachments = AttachmentClient.layer.pipe(Layer.provide(layerInfrastructure))
        const layerReplica = SqlReplica.layer(replicaOptions).pipe(
          Layer.provide(layerAttachments),
          Layer.provide(layerHandlers),
          Layer.provide(layerRemote),
          Layer.provide(layerDatabase),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(Reactivity.layer)
        )
        const context = yield* Layer.build(layerReplica)
        const replica = Context.get(context, Replica.Replica)
        const space = yield* replica.space(spaceId)

        const reference = yield* space.stageAttachment(Stream.make(hello))
        const pending = yield* space.mutate(PutMessage, {
          id: "message-1",
          body: "offline",
          attachment: reference
        })
        yield* space.releaseAttachment(reference)

        const visible = Option.getOrThrow(yield* space.get(Message, "message-1"))
        assert.deepStrictEqual(visible.attachment, reference)
        assert.deepStrictEqual(yield* collectBytes(space.readAttachment(reference)), hello)
        assert.deepStrictEqual(pending.envelope.payload, {
          id: "message-1",
          body: "offline",
          attachment: { _tag: "Attachment", digest: reference.digest, bytes: reference.bytes }
        })
        const serialized = Canonical.stringify(pending)
        assert.notInclude(serialized, "aGVsbG8=")
        assert.notInclude(serialized, "104,101,108,108,111")
      },
      provideNodeServices,
      Effect.scoped
    )
  )
})
