import * as Automerge from "@automerge/automerge"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import type * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as SchemaTransformation from "effect/SchemaTransformation"

const ImmutableString = Schema.declare(Automerge.isImmutableString, {
  identifier: "Automerge.ImmutableString"
})

const AtomicRevisionString = ImmutableString.pipe(
  Schema.decodeTo(
    Schema.String,
    SchemaTransformation.transform<string, Automerge.ImmutableString>({
      decode: (value) => value.toString(),
      encode: (value) => new Automerge.ImmutableString(value)
    })
  )
)

export const EditedMessage = Document.make("EditedMessage", {
  schema: Schema.Struct({
    body: AtomicRevisionString,
    editedAt: Schema.Number
  }),
  version: 1
})

export const chooseMessageRevision = (
  documentId: Identity.DocumentId,
  choose: (alternatives: ReadonlyArray<Conflict.Alternative>) => Conflict.Alternative
) =>
  Effect.gen(function*() {
    const replica = yield* Replica.Replica
    const inspection = yield* replica.inspectConflicts(EditedMessage, documentId)
    const bodyConflict = inspection.conflicts.find((record) =>
      record.path.parents.length === 0 &&
      record.path.target._tag === "Key" &&
      record.path.target.key === "body"
    )

    if (bodyConflict === undefined) return

    const alternative = choose(bodyConflict.alternatives)
    const commandId = yield* Identity.makeCommandId
    const resolution = {
      heads: inspection.snapshot.heads,
      path: bodyConflict.path,
      choice: {
        _tag: "SelectAlternative",
        alternativeId: alternative.id
      }
    } as const

    yield* replica.resolveConflict(EditedMessage, {
      commandId,
      documentId,
      resolution
    }).pipe(
      Effect.catchTag(
        "StaleConflictResolution",
        () => replica.inspectConflicts(EditedMessage, documentId).pipe(Effect.asVoid)
      ),
      Effect.catchReason(
        "ReplicaError",
        "CommandOutcomeUnknown",
        () =>
          replica.lookupConflictResolution(EditedMessage, {
            commandId,
            documentId,
            resolution
          }).pipe(Effect.flatMap(CommandOutcome.committedOrFail))
      )
    )
  })
