import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Backup from "../src/Backup.js"
import * as Commit from "../src/Commit.js"
import * as Identity from "../src/Identity.js"

describe("wire contracts", () => {
  const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000001")
  const documentId = Identity.documentIdFromCommandId(commandId)

  it("round trips a Commit through its schema", () => {
    const commit = {
      commandId,
      documentId,
      heads: ["a", "b"],
      sequence: Identity.CommitSequence.make(3)
    }
    const encoded = Schema.encodeSync(Commit.Commit)(commit)
    assert.deepStrictEqual(Schema.decodeUnknownSync(Commit.Commit)(encoded), commit)
  })

  it("round trips a Backup header and pins the format version", () => {
    const header = {
      formatVersion: 1 as const,
      definitionHash: "def_x",
      replicaId: Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001"),
      incarnation: Identity.ReplicaIncarnation.make(0),
      createdAt: "2020-01-01T00:00:00.000Z"
    }
    const encoded = Schema.encodeSync(Backup.Header)(header)
    assert.deepStrictEqual(Schema.decodeUnknownSync(Backup.Header)(encoded), header)
    assert.throws(() => Schema.decodeUnknownSync(Backup.FormatVersion)(2))
  })
})
