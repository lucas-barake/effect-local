import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { type ChildProcess, spawn } from "node:child_process"
import { copyFileSync, existsSync, mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { dirname, join } from "node:path"
import { StringDecoder } from "node:string_decoder"
import { fileURLToPath } from "node:url"
import { AckLineJson, ReadLineJson } from "./crash-durability/fixture.js"

const testDirectory = dirname(fileURLToPath(import.meta.url))
const packageRoot = dirname(testDirectory)
const childScript = join(testDirectory, "crash-durability", "childProcess.ts")

interface ChildCompletion {
  readonly code: number | null
  readonly signal: NodeJS.Signals | null
  readonly stdout: string
}

interface SpawnedChild {
  readonly process: ChildProcess
  readonly firstLine: Promise<string>
  readonly completion: Promise<ChildCompletion>
}

const childError = (cause: unknown) => cause instanceof Error ? cause : new Error(String(cause))

const awaitFirstLine = (child: SpawnedChild) =>
  Effect.tryPromise({
    try: () => child.firstLine,
    catch: childError
  })

const awaitCompletion = (child: SpawnedChild) => Effect.promise(() => child.completion)

const spawnChild = (args: ReadonlyArray<string>) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const child = spawn(process.execPath, ["--import", "tsx", childScript, ...args], {
        cwd: packageRoot,
        stdio: ["ignore", "pipe", "inherit"]
      })
      const decoder = new StringDecoder("utf8")
      let stdout = ""
      let firstLineSettled = false
      let resolveFirstLine: (line: string) => void
      let rejectFirstLine: (error: Error) => void
      const firstLine = new Promise<string>((resolve, reject) => {
        resolveFirstLine = resolve
        rejectFirstLine = reject
      })
      void firstLine.catch(() => {})
      const settleFirstLine = () => {
        if (firstLineSettled) return
        const newline = stdout.indexOf("\n")
        if (newline < 0) return
        firstLineSettled = true
        resolveFirstLine(stdout.slice(0, newline))
      }
      child.stdout!.on("data", (chunk: Buffer) => {
        stdout += decoder.write(chunk)
        settleFirstLine()
      })
      child.once("error", (error) => {
        if (firstLineSettled) return
        firstLineSettled = true
        rejectFirstLine(error)
      })
      const completion = new Promise<ChildCompletion>((resolve) => {
        child.once("close", (code, signal) => {
          stdout += decoder.end()
          settleFirstLine()
          if (!firstLineSettled) {
            firstLineSettled = true
            rejectFirstLine(new Error(`child closed before emitting a line (code=${code}, signal=${signal})`))
          }
          resolve({ code, signal, stdout })
        })
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
        throw new Error("child stdout must contain exactly one newline-terminated record")
      }
      return stdout.slice(0, newline)
    },
    catch: childError
  })

describe("CrashDurability", () => {
  it.live("awaits child termination before releasing its scope", () =>
    Effect.gen(function*() {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => mkdtempSync(join(tmpdir(), "effect-local-crash-cleanup-"))),
        (dir) => Effect.sync(() => rmSync(dir, { recursive: true, force: true }))
      )
      const child = yield* Effect.scoped(spawnChild(["write", join(directory, "replica.sqlite")]))

      assert.strictEqual(child.process.signalCode, "SIGKILL")
      assert.isTrue(child.process.stdout!.destroyed)
    }))

  it.live("recovers an acknowledged write after the writing process is SIGKILLed", () =>
    Effect.gen(function*() {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => mkdtempSync(join(tmpdir(), "effect-local-crash-"))),
        (dir) => Effect.sync(() => rmSync(dir, { recursive: true, force: true }))
      )
      const databasePath = join(directory, "replica.sqlite")

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
        existsSync(`${databasePath}-wal`),
        "the WAL sidecar must survive the kill; a clean close would have checkpointed it away"
      )

      const mainOnlyPath = join(directory, "main-only.sqlite")
      yield* Effect.sync(() => copyFileSync(databasePath, mainOnlyPath))
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
    }), 30_000)
})
