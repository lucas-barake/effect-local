import { NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { FileSystem } from "effect/FileSystem"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as Migrations from "../../src/Migrations.js"
import { fixturePath, fixtures } from "./versions.js"

// Regenerates the committed binary fixtures from the migration SQL at the current commit. Guarded so it
// never rewrites the frozen artifacts during a normal test run; invoke with EFFECT_LOCAL_REGEN_FIXTURES=1.
describe("migration fixtures", () => {
  it.effect.skipIf(process.env.EFFECT_LOCAL_REGEN_FIXTURES !== "1")(
    "writes a frozen database for each shipped schema version",
    () =>
      Effect.gen(function*() {
        const fs = yield* FileSystem
        const dir = yield* fs.makeTempDirectoryScoped()
        for (const spec of fixtures) {
          const staging = `${dir}/${spec.file}`
          yield* Effect.gen(function*() {
            yield* Migrator.make({})({
              loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, spec.appliedThroughId)),
              table: "effect_local_migrations"
            })
            yield* spec.seed
          }).pipe(
            Effect.provide(SqliteClient.layer({ filename: staging, disableWAL: true })),
            Effect.scoped
          )
          yield* fs.copyFile(staging, fixturePath(spec.file))
        }
      }).pipe(Effect.provide(NodeFileSystem.layer))
  )
})
