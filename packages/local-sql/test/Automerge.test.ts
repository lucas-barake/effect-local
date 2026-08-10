import * as NativeAutomerge from "@automerge/automerge"
import { assert, describe, it } from "@effect/vitest"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Clock from "effect/Clock"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { vi } from "vitest"
import * as Automerge from "../src/internal/automerge.js"
import * as Conflicts from "../src/internal/conflicts.js"
import { gateLimits } from "./fixtures/limits.js"
import { encodeJson } from "./helpers/json.js"

const automergeMock: {
  cloned: NativeAutomerge.Doc<Automerge.Root<{ title: string }>> | undefined
} = vi.hoisted(() => ({ cloned: undefined }))
vi.mock(
  "@automerge/automerge",
  (importActual) =>
    vi.importActual<typeof NativeAutomerge>("@automerge/automerge").then((actual) => ({
      ...actual,
      clone: <T,>(
        document: NativeAutomerge.Doc<T>,
        options?: NativeAutomerge.ActorId | NativeAutomerge.InitOptions<T>
      ) => {
        const cloned = actual.clone(document, options)
        automergeMock.cloned = cloned
        return cloned
      }
    }))
)

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

  const nestedLosingConflict = () => {
    const durable = Automerge.initialize({ section: { value: "base" } }, actor("10"))
    const durableHeads = Automerge.heads(durable)
    const losing = Automerge.stage(durable, actor("11"), (draft) => {
      draft.section = { value: "losing" }
    })
    const visible = Automerge.stage(durable, actor("12"), (draft) => {
      draft.section = { value: "visible" }
    })
    const losingHeads = Automerge.heads(losing)
    const left = Automerge.stage(losing, actor("13"), (draft) => {
      draft.section.value = "nested-left"
    })
    const right = Automerge.stage(losing, actor("14"), (draft) => {
      draft.section.value = "nested-right"
    })
    const nested = Automerge.replay(
      left,
      Automerge.changesSince(right, losingHeads).map((change) => change.bytes)
    )
    const merged = Automerge.replay(
      nested,
      Automerge.changesSince(visible, durableHeads).map((change) => change.bytes)
    )
    Automerge.free(durable)
    Automerge.free(losing)
    Automerge.free(visible)
    Automerge.free(right)
    return merged
  }

  it("derives stable document scoped actors", () => {
    const replicaId = Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001")
    const generation = Identity.WriterGeneration.make(1)
    const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
    const derivedActor = Automerge.actorId(replicaId, generation, documentId)
    assert.strictEqual(Automerge.actorId(replicaId, generation, documentId), derivedActor)
    assert.notStrictEqual(
      Automerge.actorId(
        replicaId,
        generation,
        Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
      ),
      derivedActor
    )
  })

  it("extracts explicit changes and replays them from durable heads", () => {
    const replicaId = Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001")
    const documentActor = Automerge.actorId(
      replicaId,
      Identity.WriterGeneration.make(1),
      Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
    )
    const durable = Automerge.initialize({ title: "one", labels: [] }, documentActor)
    const durableHeads = Automerge.heads(durable)
    const staged = Automerge.stage(durable, documentActor, (draft) => {
      draft.title = "two"
      draft.labels.push("local")
    })
    const changes = Automerge.changesSince(staged, durableHeads)
    const replayed = Automerge.replay(durable, changes.map((change) => change.bytes))
    assert.deepStrictEqual(Automerge.value(replayed), { title: "two", labels: ["local"] })
    assert.deepStrictEqual(Automerge.heads(replayed), Automerge.heads(staged))
    assert.strictEqual(changes.length, 1)
    assert.strictEqual(changes[0]?.actor, documentActor)
    Automerge.free(replayed)
    Automerge.free(staged)
  })

  it.effect("frees a staged clone when the change callback throws", () => {
    const callbackActor = "00000000000000000000000000000001"
    const durable = Automerge.initialize({ title: "one" }, callbackActor)
    automergeMock.cloned = undefined
    return Effect.sync(() => {
      assert.throws(() =>
        Automerge.stage(durable, callbackActor, () => {
          assert.fail("change rejected")
        })
      )
      assert.isDefined(automergeMock.cloned)
      if (automergeMock.cloned === undefined) return
      assert.throws(() => NativeAutomerge.getHeads(automergeMock.cloned))
    }).pipe(Effect.ensuring(Effect.sync(() => Automerge.free(durable))))
  })

  it.effect("inspects detached datatype exact alternatives and resolves a selected scalar", () =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        concurrent(
          { title: "base" },
          (draft) => draft.title = "same",
          (draft) => draft.title = new NativeAutomerge.ImmutableString("same")
        )
      ),
      (merged) =>
        Effect.gen(function*() {
          const records = yield* Conflicts.inspect(merged, gateLimits)
          assert.strictEqual(records.length, 1)
          const record = records[0]
          assert.deepStrictEqual(record.path, {
            parents: [],
            target: { _tag: "Key", key: "title" }
          })
          assert.isTrue(record.alternatives.some((alternative) => typeof alternative.value === "string"))
          const immutable = record.alternatives.find((alternative) =>
            NativeAutomerge.isImmutableString(alternative.value)
          )
          assert.isDefined(immutable)
          if (immutable === undefined) yield* Effect.die("expected an immutable alternative")
          assert.isTrue(record.alternatives.some((alternative) => alternative.id === record.visible))

          const resolution: Conflict.Resolution = {
            heads: Automerge.heads(merged),
            path: record.path,
            choice: {
              _tag: "SelectAlternative",
              alternativeId: immutable.id
            }
          }
          const prepared = yield* Conflicts.prepareResolution(merged, resolution, gateLimits)
          const resolved = Automerge.stage(merged, actor("4"), (draft) =>
            Conflicts.applyResolution(draft, prepared, { promoteParents: false }))
          yield* Effect.gen(function*() {
            const resolvedValue = Automerge.value(resolved)
            if (!NativeAutomerge.isImmutableString(resolvedValue.title)) {
              yield* Effect.die("expected an immutable resolved value")
            }
            assert.deepStrictEqual(yield* Conflicts.inspect(resolved, gateLimits), [])
            if (!NativeAutomerge.isImmutableString(immutable.value)) {
              yield* Effect.die("expected an immutable selected value")
            }
            immutable.value.val = "detached"
            assert.strictEqual(resolvedValue.title.val, "same")
          }).pipe(Effect.ensuring(Effect.sync(() =>
            Automerge.free(resolved)
          )))
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
            path: record.path,
            choice: { _tag: "DeleteValue" }
          }
          const prepared = yield* Conflicts.prepareResolution(merged, resolution, gateLimits)
          const resolved = Automerge.stage(
            merged,
            actor("5"),
            (draft) => Conflicts.applyResolution(draft, prepared, { promoteParents: false })
          )
          yield* Effect.sync(() => assert.deepStrictEqual(Automerge.value(resolved).items, [])).pipe(
            Effect.ensuring(Effect.sync(() => Automerge.free(resolved)))
          )
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))

  it.effect("accepts null prototype maps as conflict replacements", () =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        concurrent(
          { value: "base" },
          (draft) => draft.value = "left",
          (draft) => draft.value = "right"
        )
      ),
      (merged) =>
        Effect.gen(function*() {
          const [record] = yield* Conflicts.inspect(merged, gateLimits)
          assert.isDefined(record)
          const replacement: Record<string, NativeAutomerge.AutomergeValue> = Object.create(null)
          replacement.nested = Object.assign(Object.create(null), { title: "chosen" })
          const resolution: Conflict.Resolution = {
            heads: Automerge.heads(merged),
            path: record.path,
            choice: { _tag: "ReplaceValue", value: replacement }
          }
          const prepared = yield* Conflicts.prepareResolution(merged, resolution, gateLimits)
          const resolved = Automerge.stage(
            merged,
            actor("15"),
            (draft) => Conflicts.applyResolution(draft, prepared, { promoteParents: false })
          )
          yield* Effect.gen(function*() {
            assert.deepStrictEqual(Automerge.value(resolved), { value: { nested: { title: "chosen" } } })
            assert.deepStrictEqual(yield* Conflicts.inspect(resolved, gateLimits), [])
          }).pipe(Effect.ensuring(Effect.sync(() => Automerge.free(resolved))))
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))

  it.effect("keeps a composite replacement nested under the visible parent", () =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        concurrent(
          { section: { value: "base" } },
          (draft) => draft.section.value = "left",
          (draft) => draft.section.value = "right"
        )
      ),
      (merged) =>
        Effect.gen(function*() {
          const [record] = yield* Conflicts.inspect(merged, gateLimits)
          assert.isDefined(record)
          const resolution: Conflict.Resolution = {
            heads: Automerge.heads(merged),
            path: record.path,
            choice: { _tag: "ReplaceValue", value: { selected: ["nested"] } }
          }
          const prepared = yield* Conflicts.prepareResolution(merged, resolution, gateLimits)
          const resolved = Automerge.stage(
            merged,
            actor("16"),
            (draft) => Conflicts.applyResolution(draft, prepared, { promoteParents: true })
          )
          yield* Effect.gen(function*() {
            assert.deepStrictEqual(Automerge.value(resolved), {
              section: { value: { selected: ["nested"] } }
            })
            assert.deepStrictEqual(yield* Conflicts.inspect(resolved, gateLimits), [])
          }).pipe(Effect.ensuring(Effect.sync(() => Automerge.free(resolved))))
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))

  it.effect("promotes a mutated losing composite with its scalar replacement", () =>
    Effect.acquireUseRelease(
      Effect.sync(nestedLosingConflict),
      (merged) =>
        Effect.gen(function*() {
          const records = yield* Conflicts.inspect(merged, gateLimits)
          const nested = records.find((record) => record.path.parents.length === 1)
          assert.isDefined(nested)
          const resolution: Conflict.Resolution = {
            heads: Automerge.heads(merged),
            path: nested.path,
            choice: { _tag: "ReplaceValue", value: "promoted" }
          }
          const prepared = yield* Conflicts.prepareResolution(merged, resolution, gateLimits)
          const resolved = Automerge.stage(
            merged,
            actor("17"),
            (draft) => Conflicts.applyResolution(draft, prepared, { promoteParents: true })
          )
          yield* Effect.gen(function*() {
            assert.deepStrictEqual(Automerge.value(resolved), { section: { value: "promoted" } })
            assert.deepStrictEqual(yield* Conflicts.inspect(resolved, gateLimits), [])
          }).pipe(Effect.ensuring(Effect.sync(() => Automerge.free(resolved))))
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))

  it.effect("promotes a mutated losing composite with its composite replacement", () =>
    Effect.acquireUseRelease(
      Effect.sync(nestedLosingConflict),
      (merged) =>
        Effect.gen(function*() {
          const records = yield* Conflicts.inspect(merged, gateLimits)
          const nested = records.find((record) => record.path.parents.length === 1)
          assert.isDefined(nested)
          const resolution: Conflict.Resolution = {
            heads: Automerge.heads(merged),
            path: nested.path,
            choice: { _tag: "ReplaceValue", value: { selected: ["promoted"] } }
          }
          const prepared = yield* Conflicts.prepareResolution(merged, resolution, gateLimits)
          const resolved = Automerge.stage(
            merged,
            actor("18"),
            (draft) => Conflicts.applyResolution(draft, prepared, { promoteParents: true })
          )
          yield* Effect.gen(function*() {
            assert.deepStrictEqual(Automerge.value(resolved), {
              section: { value: { selected: ["promoted"] } }
            })
            assert.deepStrictEqual(yield* Conflicts.inspect(resolved, gateLimits), [])
          }).pipe(Effect.ensuring(Effect.sync(() => Automerge.free(resolved))))
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))

  it.effect("orders conflict paths by UTF16 code units", () =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        concurrent(
          { z: "base", ä: "base" },
          (draft) => {
            draft.z = "left-z"
            draft.ä = "left-ä"
          },
          (draft) => {
            draft.z = "right-z"
            draft.ä = "right-ä"
          }
        )
      ),
      (merged) =>
        Conflicts.inspect(merged, gateLimits).pipe(
          Effect.map((records) => {
            assert.deepStrictEqual(
              records.map((record) => {
                if (record.path.target._tag === "Key") return record.path.target.key
                return record.path.target.index
              }),
              ["z", "ä"]
            )
          })
        ),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))

  it.effect("rejects traversal budgets before encoded size inspection", () =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        concurrent(
          { value: "base" },
          (draft) => draft.value = "left",
          (draft) => draft.value = "right"
        )
      ),
      (merged) =>
        Effect.gen(function*() {
          const error = yield* Effect.flip(
            Conflicts.inspect(merged, {
              ...gateLimits,
              maxConflictNodes: 2,
              maxConflictValueBytes: 1
            })
          )
          assert.strictEqual(error._tag, "ReplicaError")
          if (error._tag === "ReplicaError") {
            assert.strictEqual(error.reason._tag, "QuotaExceeded")
            if (error.reason._tag === "QuotaExceeded") {
              assert.strictEqual(error.reason.resource, "conflict nodes")
              assert.strictEqual(error.reason.limit, 2)
            }
          }
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))

  it.effect("rejects large byte alternatives with bounded inspection memory", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const durable = Automerge.initialize({ value: new Uint8Array([0]) }, actor("19"))
        const heads = Automerge.heads(durable)
        const left = Automerge.stage(durable, actor("20"), (draft) => {
          draft.value = new Uint8Array(7 * 1024 * 1024).fill(1)
        })
        const right = Automerge.stage(durable, actor("21"), (draft) => {
          draft.value = new Uint8Array(7 * 1024 * 1024).fill(2)
        })
        const merged = Automerge.replay(
          left,
          Automerge.changesSince(right, heads).map((change) => change.bytes)
        )
        Automerge.free(durable)
        Automerge.free(right)
        return merged
      }),
      (merged) =>
        Effect.gen(function*() {
          const rssBefore = process.memoryUsage().rss
          const started = yield* Clock.currentTimeMillis
          const error = yield* Effect.flip(
            Conflicts.inspect(merged, {
              ...gateLimits,
              maxConflictValueBytes: 1024 * 1024
            })
          )
          const elapsedMillis = (yield* Clock.currentTimeMillis) - started
          const rssGrowth = Math.max(0, process.memoryUsage().rss - rssBefore)

          assert.strictEqual(error._tag, "ReplicaError")
          if (error._tag === "ReplicaError") {
            assert.strictEqual(error.reason._tag, "QuotaExceeded")
            if (error.reason._tag === "QuotaExceeded") {
              assert.strictEqual(error.reason.resource, "conflict value bytes")
              assert.strictEqual(error.reason.limit, 1024 * 1024)
            }
          }
          assert.isBelow(
            rssGrowth,
            128 * 1024 * 1024,
            `inspection RSS grew ${rssGrowth} bytes in ${elapsedMillis.toFixed(1)}ms`
          )
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ), 30_000)

  it.effect("enforces the exact encoded Conflict.Records byte boundary", () =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        concurrent(
          { a: "base", b: "base", c: "base", d: "base" },
          (draft) => {
            draft.a = "left-a"
            draft.b = "left-b"
            draft.c = "left-c"
            draft.d = "left-d"
          },
          (draft) => {
            draft.a = "right-a"
            draft.b = "right-b"
            draft.c = "right-c"
            draft.d = "right-d"
          }
        )
      ),
      (merged) =>
        Effect.gen(function*() {
          const records = yield* Conflicts.inspect(merged, gateLimits)
          assert.strictEqual(records.length, 4)
          const encoded = Schema.encodeSync(Conflict.Records)(records)
          const bytes = new TextEncoder().encode(encodeJson(encoded)).byteLength
          assert.strictEqual(
            (yield* Conflicts.inspect(merged, { ...gateLimits, maxConflictValueBytes: bytes })).length,
            records.length
          )
          const error = yield* Effect.flip(
            Conflicts.inspect(merged, { ...gateLimits, maxConflictValueBytes: bytes - 1 })
          )
          assert.strictEqual(error._tag, "ReplicaError")
          if (error._tag === "ReplicaError") {
            assert.strictEqual(error.reason._tag, "QuotaExceeded")
            if (error.reason._tag === "QuotaExceeded") {
              assert.strictEqual(error.reason.resource, "conflict value bytes")
              assert.strictEqual(error.reason.limit, bytes - 1)
            }
          }
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))

  it.effect("charges Uint8Array and Date expansion before returning conflicts to the browser wire", () =>
    Effect.acquireUseRelease(
      Effect.sync(() =>
        concurrent(
          {
            value: {
              bytes: new Uint8Array([0, 1, 2, 3, 254, 255]),
              timestamp: DateTime.toDate(DateTime.makeUnsafe("2026-07-29T12:34:56.789Z"))
            }
          },
          (draft) => {
            draft.value = {
              bytes: new Uint8Array([4, 5, 6, 7, 252, 253]),
              timestamp: DateTime.toDate(DateTime.makeUnsafe("2026-07-30T12:34:56.789Z"))
            }
          },
          (draft) => {
            draft.value = {
              bytes: new Uint8Array([8, 9, 10, 11, 250, 251]),
              timestamp: DateTime.toDate(DateTime.makeUnsafe("2026-07-31T12:34:56.789Z"))
            }
          }
        )
      ),
      (merged) =>
        Effect.gen(function*() {
          const records = yield* Conflicts.inspect(merged, gateLimits)
          assert.strictEqual(records.length, 1)
          assert.isTrue(
            records[0].alternatives.every((alternative) => {
              const value = alternative.value
              return typeof value === "object" && value !== null &&
                "bytes" in value && value.bytes instanceof Uint8Array &&
                "timestamp" in value && value.timestamp instanceof Date
            })
          )
          const nativeBytes = new TextEncoder().encode(encodeJson(records)).byteLength
          const encoded = Schema.encodeSync(Conflict.Records)(records)
          const encodedBytes = new TextEncoder().encode(encodeJson(encoded)).byteLength
          assert.isAbove(encodedBytes, nativeBytes)
          const error = yield* Effect.flip(
            Conflicts.inspect(merged, {
              ...gateLimits,
              maxConflictValueBytes: encodedBytes - 1
            })
          )
          assert.strictEqual(error._tag, "ReplicaError")
          if (error._tag === "ReplicaError") {
            assert.strictEqual(error.reason._tag, "QuotaExceeded")
            if (error.reason._tag === "QuotaExceeded") {
              assert.strictEqual(error.reason.resource, "conflict value bytes")
              assert.strictEqual(error.reason.limit, encodedBytes - 1)
            }
          }
        }),
      (merged) => Effect.sync(() => Automerge.free(merged))
    ))
})
