import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Automerge from "../src/internal/automerge.js"

describe("Automerge persistence", () => {
  it("derives stable document scoped actors", () => {
    const replicaId = Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001")
    const generation = Identity.WriterGeneration.make(1)
    const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
    const actor = Automerge.actorId(replicaId, generation, documentId)
    assert.strictEqual(Automerge.actorId(replicaId, generation, documentId), actor)
    assert.notStrictEqual(
      Automerge.actorId(
        replicaId,
        generation,
        Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
      ),
      actor
    )
  })

  it("extracts explicit changes and replays them from durable heads", () => {
    const replicaId = Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001")
    const actor = Automerge.actorId(
      replicaId,
      Identity.WriterGeneration.make(1),
      Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
    )
    const durable = Automerge.initialize({ title: "one", labels: [] as Array<string> }, actor)
    const durableHeads = Automerge.heads(durable)
    const staged = Automerge.stage(durable, actor, (draft) => {
      draft.title = "two"
      draft.labels.push("local")
    })
    const changes = Automerge.changesSince(staged, durableHeads)
    const replayed = Automerge.replay(durable, changes.map((change) => change.bytes))
    assert.deepStrictEqual(Automerge.value(replayed), { title: "two", labels: ["local"] })
    assert.deepStrictEqual(Automerge.heads(replayed), Automerge.heads(staged))
    assert.strictEqual(changes.length, 1)
    assert.strictEqual(changes[0]?.actor, actor)
    Automerge.free(replayed)
    Automerge.free(staged)
  })
})
