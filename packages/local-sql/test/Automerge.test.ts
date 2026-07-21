import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Automerge from "../src/internal/automerge.js"

describe("Automerge persistence", () => {
  it("derives stable document scoped actors", () => {
    const replicaId = Identity.makeReplicaId()
    const generation = Identity.WriterGeneration.make(1)
    const documentId = Identity.makeDocumentId()
    const actor = Automerge.actorId(replicaId, generation, documentId)
    assert.strictEqual(Automerge.actorId(replicaId, generation, documentId), actor)
    assert.notStrictEqual(Automerge.actorId(replicaId, generation, Identity.makeDocumentId()), actor)
  })

  it("extracts explicit changes and replays them from durable heads", () => {
    const replicaId = Identity.makeReplicaId()
    const actor = Automerge.actorId(
      replicaId,
      Identity.WriterGeneration.make(1),
      Identity.makeDocumentId()
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
