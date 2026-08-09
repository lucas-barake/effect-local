import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as SqlReplica from "../src/SqlReplica.js"
import { gateLimits } from "./fixtures/limits.js"

const Task = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String }),
  version: 1
})

const Rename = Mutation.make("Rename", {
  document: Task,
  payload: { title: Schema.String }
})

const definition = ReplicaDefinition.make({
  name: "commit-publisher-poller",
  documents: DocumentSet.make(Task),
  mutations: [Rename],
  projections: [],
  queries: []
})

const Live = SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
  Layer.provideMerge(Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    ReplicaLimits.layer(gateLimits),
    Rename.toLayer(({ draft, payload }) => {
      draft.title = payload.title
      return undefined
    })
  ))
)

it.layer(Live, { timeout: 60_000, excludeTestServices: true })("CommitPublisher poller interrupt", (it) => {
  /**
   * Every attached tab holds one commit subscription through the owner's invalidation stream, and
   * those streams are interrupted routinely (session replacement, tab teardown, stream retry). One
   * interrupted poll must not stop the publisher from serving the remaining tabs.
   */
  it.effect("keeps publishing commits after one subscriber's pending poll is interrupted", () =>
    Effect.gen(function*() {
      const publisher = yield* CommitPublisher.CommitPublisher
      const replica = yield* Replica.Replica

      const subscription = yield* publisher.subscribe
      const parked = yield* Effect.forkChild(Stream.runHead(subscription.events), { startImmediately: true })
      yield* Effect.yieldNow
      yield* Fiber.interrupt(parked)

      const exit = yield* Effect.exit(
        replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "first" }
        })
      )
      assert.isTrue(
        Exit.isSuccess(exit),
        `a commit after an interrupted subscriber poll must not die: ${Exit.isSuccess(exit) ? "" : String(exit.cause)}`
      )
    }))
})
