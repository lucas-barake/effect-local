import { NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Config from "effect/Config"
import * as ConfigProvider from "effect/ConfigProvider"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Latch from "effect/Latch"
import type * as PlatformError from "effect/PlatformError"
import * as Schema from "effect/Schema"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Migrations from "../../src/Migrations.js"
import type { FixtureSpec } from "./versions.js"
import { fixtureDirectory, fixtures } from "./versions.js"

const fixedMigrationCreatedAt = "2020-01-01 00:00:00"
const generatorRuntime = { node: "24.15.0", sqlite: "3.51.3" } satisfies Readonly<Record<"node" | "sqlite", string>>

const generateFixture = (spec: FixtureSpec, database: string) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* Migrator.make({})({
      loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, spec.appliedThroughId)),
      table: "effect_local_migrations"
    })
    yield* spec.seed
    yield* sql`UPDATE effect_local_migrations SET created_at = ${fixedMigrationCreatedAt}`

    const integrity = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ integrity_check: Schema.String }),
      execute: () => sql`PRAGMA integrity_check`
    })(undefined)
    assert.deepStrictEqual(integrity, [{ integrity_check: "ok" }])

    const foreignKeyViolations = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({
        table: Schema.String,
        rowid: Schema.NullOr(Schema.Int),
        parent: Schema.String,
        fkid: Schema.Int
      }),
      execute: () => sql`PRAGMA foreign_key_check`
    })(undefined)
    assert.deepStrictEqual(foreignKeyViolations, [])
    yield* sql`VACUUM`
  }).pipe(
    Effect.provide(SqliteClient.layer({ filename: database, disableWAL: true })),
    Effect.scoped
  )

const generateFixtureSet = (directory: string, specs: ReadonlyArray<FixtureSpec>) =>
  Effect.forEach(
    specs,
    (spec) => generateFixture(spec, `${directory}/${spec.file}`),
    { discard: true }
  )

// The gate holds back every link after the first, so interruption is tested at one
// deterministic interleaving instead of racing real time.
interface LinkProbe {
  readonly awaitStarted: (index: number) => Effect.Effect<void>
  readonly awaitCompleted: (index: number) => Effect.Effect<void>
  readonly awaitQuiescent: Effect.Effect<void>
  readonly release: Effect.Effect<void>
  readonly link: (
    index: number,
    path: string,
    destination: string
  ) => Effect.Effect<void, PlatformError.PlatformError>
}

const makeLinkProbe = (fs: FileSystem.FileSystem): LinkProbe => {
  const started = fixtures.map(() => Latch.makeUnsafe())
  const completed = fixtures.map(() => Latch.makeUnsafe())
  const quiescent = Latch.makeUnsafe(true)
  const releaseGate = Latch.makeUnsafe()
  let inFlight = 0
  return {
    awaitStarted: (index) => started[index].await,
    awaitCompleted: (index) => completed[index].await,
    awaitQuiescent: quiescent.await,
    release: Effect.sync(() => Latch.openUnsafe(releaseGate)),
    link: (index, path, destination) =>
      Effect.gen(function*() {
        inFlight++
        Latch.closeUnsafe(quiescent)
        Latch.openUnsafe(started[index])
        if (index > 0) yield* releaseGate.await
        yield* fs.link(path, destination)
        inFlight--
        if (inFlight === 0) Latch.openUnsafe(quiescent)
        Latch.openUnsafe(completed[index])
      })
  }
}

const publishMissingFixtures = (destinationDirectory: string, probe?: LinkProbe) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const existing = yield* Effect.forEach(
      fixtures,
      (spec) => fs.exists(`${destinationDirectory}/${spec.file}`)
    )
    const firstMissing = existing.indexOf(false)
    if (firstMissing === -1) return []
    if (existing.slice(firstMissing).some(Boolean)) {
      return yield* Effect.die("Existing migration fixtures must form a complete version prefix")
    }

    const missing = fixtures.slice(firstMissing)
    const stagingDirectory = yield* fs.makeTempDirectoryScoped()
    yield* generateFixtureSet(stagingDirectory, missing)

    const pending = yield* Effect.forEach(missing, (spec) =>
      Effect.gen(function*() {
        const path = yield* fs.makeTempFileScoped({
          directory: destinationDirectory,
          prefix: `.${spec.file}.`,
          suffix: ".tmp"
        })
        yield* fs.copyFile(`${stagingDirectory}/${spec.file}`, path)
        return { path, published: `${destinationDirectory}/${spec.file}` }
      }))
    yield* Effect.yieldNow

    // Uninterruptible: the link resumes through a callback with no canceler, so an interrupted fiber
    // is abandoned while the syscall keeps going and publishes afterwards, leaving the directory ahead
    // of what this run reported. The `yieldNow` is now the only interruption point.
    yield* Effect.forEach(
      pending,
      ({ path, published: destination }, index) =>
        Effect.uninterruptible(Effect.gen(function*() {
          if (probe === undefined) return yield* fs.link(path, destination)
          return yield* probe.link(index, path, destination)
        })).pipe(
          Effect.andThen(Effect.yieldNow)
        ),
      { discard: true }
    )
    return missing.map((spec) => spec.file)
  })

describe("migration fixture generation", () => {
  it.effect("normalizes volatile metadata within one SQLite runtime", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const first = yield* fs.makeTempDirectoryScoped()
      const second = yield* fs.makeTempDirectoryScoped()
      yield* generateFixtureSet(first, fixtures)
      yield* generateFixtureSet(second, fixtures)

      for (const spec of fixtures) {
        assert.deepStrictEqual(
          yield* fs.readFile(`${first}/${spec.file}`),
          yield* fs.readFile(`${second}/${spec.file}`)
        )
      }
    }).pipe(Effect.provide(NodeFileSystem.layer)))

  it.effect("publishes a complete missing suffix without replacing frozen fixtures", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      assert.deepStrictEqual(yield* publishMissingFixtures(directory), fixtures.map((spec) => spec.file))

      const frozen = `${directory}/${fixtures[0].file}`
      yield* fs.writeFile(frozen, Uint8Array.of(99))
      assert.deepStrictEqual(yield* publishMissingFixtures(directory), [])
      assert.deepStrictEqual(yield* fs.readFile(frozen), Uint8Array.of(99))
    }).pipe(Effect.provide(NodeFileSystem.layer)))

  it.effect("leaves an interrupted publication as a recoverable version prefix", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const publisher = yield* publishMissingFixtures(directory).pipe(Effect.forkChild)
      const first = `${directory}/${fixtures[0].file}`
      while (!(yield* fs.exists(first))) yield* Effect.yieldNow
      yield* Fiber.interrupt(publisher)

      const existing = yield* Effect.forEach(
        fixtures,
        (spec) => fs.exists(`${directory}/${spec.file}`)
      )
      assert.isTrue(existing[0])
      for (let index = 1; index < existing.length; index++) {
        if (!existing[index - 1]) assert.isFalse(existing[index])
      }

      yield* publishMissingFixtures(directory)
      for (const spec of fixtures) assert.isTrue(yield* fs.exists(`${directory}/${spec.file}`))
    }).pipe(Effect.provide(NodeFileSystem.layer)))

  it.effect("publishes nothing more once it reports the publication was interrupted", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const probe = makeLinkProbe(fs)
      const publisher = yield* publishMissingFixtures(directory, probe).pipe(Effect.forkChild)

      // Interrupt with the second link mid flight, the one interleaving where a cancel-less
      // syscall could outlive the fiber that reports the interruption.
      yield* probe.awaitCompleted(0)
      yield* probe.awaitStarted(1)
      const interruption = yield* Fiber.interrupt(publisher).pipe(Effect.forkChild)
      yield* probe.release
      yield* Fiber.join(interruption)

      const published = (files: ReadonlyArray<string>) =>
        files.filter((file) => fixtures.some((spec) => spec.file === file)).toSorted()
      const reported = published(yield* fs.readDirectory(directory))
      yield* probe.awaitQuiescent
      assert.deepStrictEqual(
        published(yield* fs.readDirectory(directory)),
        reported,
        "an interrupted publication must leave the directory exactly as it reported it"
      )
    }).pipe(Effect.provide(NodeFileSystem.layer)))

  it.effect("does not replace a fixture published concurrently after the prefix snapshot", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const publisher = yield* publishMissingFixtures(directory).pipe(Effect.exit, Effect.forkChild)
      const first = fixtures[0]
      const pendingPrefix = `.${first.file}.`
      while (!(yield* fs.readDirectory(directory)).some((file) => file.startsWith(pendingPrefix))) {
        yield* Effect.yieldNow
      }

      const frozen = Uint8Array.of(99)
      yield* fs.writeFile(`${directory}/${first.file}`, frozen)
      assert.strictEqual((yield* Fiber.join(publisher))._tag, "Failure")
      assert.deepStrictEqual(yield* fs.readFile(`${directory}/${first.file}`), frozen)
    }).pipe(Effect.provide(NodeFileSystem.layer)))

  it.effect("refuses a fixture gap before publishing any missing database", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      yield* fs.writeFile(`${directory}/${fixtures[1].file}`, Uint8Array.of(1))

      assert.strictEqual((yield* Effect.exit(publishMissingFixtures(directory)))._tag, "Failure")
      assert.isFalse(yield* fs.exists(`${directory}/${fixtures[0].file}`))
      assert.isFalse(yield* fs.exists(`${directory}/${fixtures[2].file}`))
    }).pipe(Effect.provide(NodeFileSystem.layer)))

  it.effect.skipIf(
    Effect.runSync(
      Config.string("EFFECT_LOCAL_REGEN_FIXTURES").pipe(Config.withDefault("0")).parse(ConfigProvider.fromEnv())
    ) !== "1"
  )(
    "publishes only missing trailing frozen fixtures",
    () =>
      Effect.gen(function*() {
        assert.strictEqual(process.versions.node, generatorRuntime.node)
        assert.strictEqual(process.versions.sqlite, generatorRuntime.sqlite)
        yield* publishMissingFixtures(fixtureDirectory)
      }).pipe(Effect.provide(NodeFileSystem.layer))
  )
})
