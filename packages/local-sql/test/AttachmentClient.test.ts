import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as AttachmentClient from "../src/AttachmentClient.js"
import * as AttachmentStorage from "../src/AttachmentStorage.js"
import * as AttachmentTransfer from "../src/AttachmentTransfer.js"
import * as FileSystemAttachmentStorage from "../src/FileSystemAttachmentStorage.js"
import * as Migrations from "../src/Migrations.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")
const hello = Uint8Array.from([104, 101, 108, 108, 111])
const layerNodeServices = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
const provideNodeServices = Effect.provide(layerNodeServices)

const collectBytes = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

describe("attachment client", () => {
  it.effect(
    "revalidates remote custody before retrying a pending attachment",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-client-custody-" })
        const remote = yield* Ref.make<Uint8Array | undefined>(undefined)
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true }).pipe(
          Layer.provide(Reactivity.layer)
        )
        const layerStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/objects`,
          maximumBytes: 8
        })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: (request) =>
              collectBytes(request.bytes(0)).pipe(
                Effect.flatMap((bytes) => Ref.set(remote, bytes))
              ),
            download: () => Stream.fail(new ReplicaError.ServerUnavailable())
          })
        )
        const context = yield* AttachmentClient.layer.pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
        const sql = Context.get(context, SqlClient.SqlClient)
        const client = Context.get(context, AttachmentClient.AttachmentClient)
        yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )
        const reference = yield* client.stage(spaceId, Stream.make(hello))

        yield* client.ensureUploaded(spaceId, clientId, membershipIncarnation, [reference])
        assert.deepStrictEqual(yield* Ref.get(remote), hello)

        yield* Ref.set(remote, undefined)
        yield* client.ensureUploaded(spaceId, clientId, membershipIncarnation, [reference])

        assert.deepStrictEqual(yield* Ref.get(remote), hello)
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "keeps offline staged bytes across restart and releases them after pending settlement",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-client-" })
        const database = `${root}/client.sqlite`
        const objects = `${root}/objects`
        const downloads = yield* Ref.make(0)
        const layerDatabase = SqliteClient.layer({ filename: database, disableWAL: true }).pipe(
          Layer.provide(Reactivity.layer)
        )
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: objects, maximumBytes: 8 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: () => Effect.fail(new ReplicaError.ServerUnavailable()),
            download: () =>
              Stream.fromEffect(
                Ref.update(downloads, (count) => count + 1).pipe(Effect.as(hello))
              )
          })
        )
        const layerClient = AttachmentClient.layer.pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer)
        )
        const layerRuntime = Layer.merge(layerInfrastructure, layerClient)

        const firstScope = yield* Scope.make()
        const first = yield* Layer.buildWithScope(layerRuntime, firstScope)
        const sql = Context.get(first, SqlClient.SqlClient)
        const client = Context.get(first, AttachmentClient.AttachmentClient)
        yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )
        const reference = yield* client.stage(spaceId, Stream.make(hello))
        assert.strictEqual(reference.bytes, hello.length)
        yield* Scope.close(firstScope, Exit.void)

        const secondScope = yield* Scope.make()
        const second = yield* Layer.buildWithScope(layerRuntime, secondScope)
        const restarted = Context.get(second, AttachmentClient.AttachmentClient)
        const restartedSql = Context.get(second, SqlClient.SqlClient)
        assert.deepStrictEqual(
          yield* collectBytes(restarted.read(spaceId, clientId, membershipIncarnation, reference)),
          hello
        )
        const objectKey = yield* restarted.objectKey(spaceId, reference)

        yield* restartedSql.withTransaction(Effect.gen(function*() {
          yield* restartedSql`INSERT INTO effect_local_client_pending_data
            (space_id, schema_generation, membership_incarnation, mutation_id, local_sequence, basis,
              name, payload_json, digest, digest_version, source_schema_version, source_schema_hash,
              mutation_version, optimistic_result_json, changes_json)
            VALUES (${spaceId}, 0, ${membershipIncarnation}, ${mutationId}, 1, 0,
              'PutTodo', '{}', ${"0".repeat(64)}, 3, ${Domain.definition.schemaIdentity.version},
              ${Domain.definition.schemaIdentity.hash}, 1, 'null', '[]')`
          yield* restarted.associatePending(spaceId, mutationId, [reference])
        }))
        yield* restarted.release(spaceId, reference)
        yield* restartedSql`DELETE FROM effect_local_client_pending_data WHERE mutation_id = ${mutationId}`
        assert.strictEqual(yield* restarted.drainDeletions(8), 1)
        const stored = Context.get(second, AttachmentStorage.AttachmentStorage)
        assert.strictEqual(yield* stored.exists(objectKey), false)
        assert.deepStrictEqual(
          yield* collectBytes(restarted.read(spaceId, clientId, membershipIncarnation, reference)),
          hello
        )
        assert.deepStrictEqual(
          yield* collectBytes(restarted.read(spaceId, clientId, membershipIncarnation, reference)),
          hello
        )
        assert.strictEqual(yield* Ref.get(downloads), 1)
        yield* Scope.close(secondScope, Exit.void)
      },
      provideNodeServices,
      Effect.scoped
    )
  )
})
