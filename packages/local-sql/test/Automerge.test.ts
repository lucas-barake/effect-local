import * as NativeAutomerge from "@automerge/automerge"
import { assert, describe, it } from "@effect/vitest"
import type * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Effect from "effect/Effect"
import { vi } from "vitest"
import * as Automerge from "../src/internal/automerge.js"
import * as Conflicts from "../src/internal/conflicts.js"
import { gateLimits } from "./fixtures/limits.js"

const automergeMock = vi.hoisted(() => ({
  cloned: undefined as NativeAutomerge.Doc<Automerge.Root<{ title: string }>> | undefined
}))
vi.mock("@automerge/automerge", async (importActual) => {
  const actual = await importActual<typeof NativeAutomerge>()
  return {
    ...actual,
    clone: <T,>(
      document: NativeAutomerge.Doc<T>,
      options?: NativeAutomerge.ActorId | NativeAutomerge.InitOptions<T>
    ) => {
      const cloned = actual.clone(document, options)
      automergeMock.cloned = cloned as NativeAutomerge.Doc<Automerge.Root<{ title: string }>>
      return cloned
    }
  }
})

describe("Automerge persistence", () => {
  const actor = (suffix: string) => suffix.padStart(32, "0")

  const concurrent = <E,>(
    initial: E,
    leftChange: (draft: Mutation.DraftValue<E>) => void,
    rightChange: (draft: Mutation.DraftValue<E>) => void
  ) => {
    const durable = Automerge.initialize(initial, actor("1"))
    const heads = Automerge.heads(durable)
    const left = Automerge.stage(durable, actor("2"), leftChange)
    const right = Automerge.stage(durable, actor("3"), rightChange)
    const merged = Automerge.replay(left, Automerge.changesSince(right, heads).map((change) => change.bytes))
    Automerge.free(durable)
    Automerge.free(right)
    return merged
  }

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

  it("frees a staged clone when the change callback throws", () => {
    const actor = "00000000000000000000000000000001"
    const durable = Automerge.initialize({ title: "one" }, actor)
    automergeMock.cloned = undefined
    try {
      assert.throws(() =>
        Automerge.stage(durable, actor, () => {
          throw new Error("change rejected")
        })
      )
      assert.isDefined(automergeMock.cloned)
      assert.throws(() => NativeAutomerge.getHeads(automergeMock.cloned!))
    } finally {
      Automerge.free(durable)
    }
  })

  it.effect("inspects detached datatype exact alternatives and resolves a selected scalar", () =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        concurrent(
          { title: "base" as string | NativeAutomerge.ImmutableString },
          (draft) => draft.title = "same",
          (draft) => draft.title = new NativeAutomerge.ImmutableString("same")
        )
      ),
      (merged) =>
        Effect.gen(function*() {
          const records = yield* Conflicts.inspect(merged, gateLimits)
          assert.strictEqual(records.length, 1)
          const record = records[0]!
          assert.deepStrictEqual(record.path, {
            parents: [],
            target: { _tag: "Key", key: "title" }
          })
          assert.isTrue(record.alternatives.some((alternative) => typeof alternative.value === "string"))
          const immutable = record.alternatives.find((alternative) =>
            NativeAutomerge.isImmutableString(alternative.value)
          )
          assert.isDefined(immutable)
          assert.isTrue(record.alternatives.some((alternative) => alternative.id === record.visible))

          const resolution: Conflict.Resolution = {
            heads: Automerge.heads(merged),
            path: record.path,
            choice: {
              _tag: "SelectAlternative",
              alternativeId: immutable!.id
            }
          }
          const prepared = yield* Conflicts.prepareResolution(merged, resolution, gateLimits)
          const resolved = Automerge.stage(merged, actor("4"), (draft) =>
            Conflicts.applyResolution(draft, prepared, { promoteParents: false }))
          try {
            assert.isTrue(NativeAutomerge.isImmutableString(Automerge.value(resolved).title))
            assert.deepStrictEqual(yield* Conflicts.inspect(resolved, gateLimits), [])
            ;(immutable!.value as NativeAutomerge.ImmutableString).val = "detached"
            assert.strictEqual((Automerge.value(resolved).title as NativeAutomerge.ImmutableString).val, "same")
          } finally {
            Automerge.free(resolved)
          }
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))

  it.effect("deletes a conflicted list element with splice", () =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        concurrent(
          { items: ["base"] },
          (draft) => draft.items[0] = "left",
          (draft) => draft.items[0] = "right"
        )
      ),
      (merged) =>
        Effect.gen(function*() {
          const [record] = yield* Conflicts.inspect(merged, gateLimits)
          assert.isDefined(record)
          const resolution: Conflict.Resolution = {
            heads: Automerge.heads(merged),
            path: record!.path,
            choice: { _tag: "DeleteValue" }
          }
          const prepared = yield* Conflicts.prepareResolution(merged, resolution, gateLimits)
          const resolved = Automerge.stage(
            merged,
            actor("5"),
            (draft) => Conflicts.applyResolution(draft, prepared, { promoteParents: false })
          )
          try {
            assert.deepStrictEqual(Automerge.value(resolved).items, [])
          } finally {
            Automerge.free(resolved)
          }
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))
})
