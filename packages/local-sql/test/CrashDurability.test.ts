import { NodeFileSystem, NodePath } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Path from "effect/Path"
import * as Schema from "effect/Schema"
import { nativeError } from "./helpers/json.js"
// Effect's process services do not expose the raw ChildProcess kill and stdout lifecycle
// needed to assert SIGKILL cleanup in this crash harness.
// oxlint-disable-next-line effect/noNodeBuiltinImport
import { type ChildProcess, spawn } from "node:child_process"
import { StringDecoder } from "node:string_decoder"
import { fileURLToPath } from "node:url"
import { AckLineJson, ReadLineJson } from "./crash-durability/fixture.js"

const packageRoot = fileURLToPath(new URL("..", import.meta.url))
const childScript = fileURLToPath(new URL("./crash-durability/childProcess.ts", import.meta.url))
const NodeServices = Layer.merge(NodeFileSystem.layer, NodePath.layer)

interface ChildCompletion {
  readonly code: number | null
  readonly signal: NodeJS.Signals | null
  readonly stdout: string
}

interface SpawnedChild {
  readonly process: ChildProcess
  readonly firstLine: Deferred.Deferred<string, Error>
  readonly completion: Deferred.Deferred<ChildCompletion>
}

const childError = (cause: unknown) => {
  if (cause instanceof Error) return cause
  return nativeError(String(cause))
}

const awaitFirstLine = (child: SpawnedChild) => Deferred.await(child.firstLine)

const awaitCompletion = (child: SpawnedChild) => Deferred.await(child.completion)

const spawnChild = (args: ReadonlyArray<string>) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const child = spawn(process.execPath, ["--import", "tsx", childScript, ...args], {
        cwd: packageRoot,
        stdio: ["ignore", "pipe", "inherit"]
      })
      const firstLine = Deferred.makeUnsafe<string, Error>()
      const completion = Deferred.makeUnsafe<ChildCompletion>()
      const decoder = new StringDecoder("utf8")
      let stdout = ""
      let firstLineSettled = false
      const settleFirstLine = () => {
        if (firstLineSettled) return
        const newline = stdout.indexOf("\n")
        if (newline < 0) return
        firstLineSettled = true
        Deferred.doneUnsafe(firstLine, Effect.succeed(stdout.slice(0, newline)))
      }
      child.stdout.on("data", (chunk: Buffer) => {
        stdout += decoder.write(chunk)
        settleFirstLine()
      })
      child.once("error", (error) => {
        if (firstLineSettled) return
        firstLineSettled = true
        Deferred.doneUnsafe(firstLine, Effect.fail(error))
      })
      child.once("close", (code, signal) => {
        stdout += decoder.end()
        settleFirstLine()
        if (!firstLineSettled) {
          firstLineSettled = true
          Deferred.doneUnsafe(
            firstLine,
            Effect.fail(nativeError(`child closed before emitting a line (code=${code}, signal=${signal})`))
          )
        }
        Deferred.doneUnsafe(completion, Effect.succeed({ code, signal, stdout }))
      })
      return { process: child, firstLine, completion } satisfies SpawnedChild
    }),
    (child) =>
      Effect.gen(function*() {
        if (child.process.exitCode === null && child.process.signalCode === null) {
          yield* Effect.sync(() => {
            child.process.kill("SIGKILL")
          })
        }
        yield* awaitCompletion(child)
      })
  )

const onlyOutputLine = (stdout: string) =>
  Effect.try({
    try: () => {
      const newline = stdout.indexOf("\n")
      if (newline < 0 || newline !== stdout.length - 1) {
        return assert.fail("child stdout must contain exactly one newline-terminated record")
      }
      return stdout.slice(0, newline)
    },
    catch: childError
  })

describe("CrashDurability", () => {
  it.live("awaits child termination before releasing its scope", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-crash-cleanup-" })
      const child = yield* Effect.scoped(spawnChild(["write", path.join(directory, "replica.sqlite")]))

      assert.strictEqual(child.process.signalCode, "SIGKILL")
      assert.isTrue(child.process.stdout.destroyed)
    }).pipe(Effect.provide(NodeServices)))

  it.live("recovers an acknowledged write after the writing process is SIGKILLed", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-crash-" })
      const databasePath = path.join(directory, "replica.sqlite")

      const writer = yield* spawnChild(["write", databasePath])
      const ackLine = yield* awaitFirstLine(writer)
      const ack = yield* Schema.decodeUnknownEffect(AckLineJson)(ackLine)
      assert.strictEqual(ack.journalMode, "wal")

      yield* Effect.sync(() => writer.process.kill("SIGKILL"))
      const writerExit = yield* awaitCompletion(writer)
      assert.strictEqual(writerExit.signal, "SIGKILL")
      assert.isNull(writerExit.code)
      assert.strictEqual(yield* onlyOutputLine(writerExit.stdout), ackLine)

      assert.isTrue(
        yield* fs.exists(`${databasePath}-wal`),
        "the WAL sidecar must survive the kill; a clean close would have checkpointed it away"
      )

      const mainOnlyPath = path.join(directory, "main-only.sqlite")
      yield* fs.copyFile(databasePath, mainOnlyPath)
      const mainOnlyReader = yield* spawnChild(["read", mainOnlyPath, ack.documentId])
      const mainOnlyExit = yield* awaitCompletion(mainOnlyReader)
      assert.strictEqual(mainOnlyExit.code, 0)
      assert.isNull(mainOnlyExit.signal)
      const mainOnly = yield* Schema.decodeUnknownEffect(ReadLineJson)(
        yield* onlyOutputLine(mainOnlyExit.stdout)
      )
      assert.isFalse(mainOnly.found)
      assert.isNull(mainOnly.title)

      const reader = yield* spawnChild(["read", databasePath, ack.documentId])
      const readerExit = yield* awaitCompletion(reader)
      assert.strictEqual(readerExit.code, 0)
      assert.isNull(readerExit.signal)
      const recovered = yield* Schema.decodeUnknownEffect(ReadLineJson)(yield* onlyOutputLine(readerExit.stdout))
      assert.isTrue(recovered.found)
      assert.strictEqual(recovered.title, ack.title)
    }).pipe(Effect.provide(NodeServices)), 30_000)
})
