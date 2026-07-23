import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { vi } from "vitest"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

const automergeMock = vi.hoisted(() => ({ freeCount: 0 }))
vi.mock("../src/internal/automerge.js", async (importActual) => {
  const actual = await importActual<typeof InternalAutomerge>()
  return {
    ...actual,
    free: (document: InternalAutomerge.AnyDocument) => {
      automergeMock.freeCount++
      return actual.free(document)
    }
  }
})

describe("Recovery interruption", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.merge(Database, Bootstrap)
  const Gate = ReplicaGate.layer.pipe(Layer.provide(Base))
  const StoreService = DocumentStore.layer.pipe(Layer.provide(Layer.merge(Base, Gate)))
  const RecoveryService = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Base, Gate)))
  const Services = Layer.mergeAll(Base, Gate, StoreService, RecoveryService)

  it.effect("frees the recovered document when interrupted during decode", () =>
    Effect.gen(function*() {
      const control: {
        active: boolean
        entered?: Deferred.Deferred<void>
        release?: Deferred.Deferred<void>
      } = { active: false }
      const Suspending = Document.make("Suspending", {
        schema: Schema.Struct({ title: Schema.String }).pipe(
          Schema.middlewareDecoding((effect) =>
            control.active && control.entered && control.release
              ? Deferred.succeed(control.entered, undefined).pipe(
                Effect.andThen(Deferred.await(control.release)),
                Effect.andThen(effect)
              )
              : effect
          )
        ),
        version: 1
      })
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Suspending, documentId, { title: "leak" })
      InternalAutomerge.free(created.automerge)

      control.entered = yield* Deferred.make<void>()
      control.release = yield* Deferred.make<void>()
      control.active = true
      automergeMock.freeCount = 0

      const fiber = yield* Effect.forkChild(recovery.recover(Suspending, documentId))
      yield* Deferred.await(control.entered)
      yield* Fiber.interrupt(fiber)

      assert.strictEqual(automergeMock.freeCount, 1)
    }).pipe(Effect.provide(Services)))
})
