import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { type ChildProcess, spawn } from "node:child_process"
import { existsSync, mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { AckLineJson, ReadLineJson } from "./crash-durability/fixture.js"

const testDirectory = dirname(fileURLToPath(import.meta.url))
const packageRoot = dirname(testDirectory)
const childScript = join(testDirectory, "crash-durability", "childProcess.ts")

const spawnChild = (args: ReadonlyArray<string>) =>
  Effect.acquireRelease(
    Effect.sync(() =>
      spawn(process.execPath, ["--import", "tsx", childScript, ...args], {
        cwd: packageRoot,
        stdio: ["ignore", "pipe", "inherit"]
      })
    ),
    (child) =>
      Effect.sync(() => {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL")
      })
  )

const awaitFirstLine = (child: ChildProcess) =>
  Effect.callback<string, Error>((resume) => {
    const stdout = child.stdout!
    let buffer = ""
    const onData = (chunk: Buffer) => {
      buffer += chunk.toString("utf8")
      const newline = buffer.indexOf("\n")
      if (newline >= 0) {
        cleanup()
        resume(Effect.succeed(buffer.slice(0, newline)))
      }
    }
    const onError = (error: Error) => {
      cleanup()
      resume(Effect.fail(error))
    }
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup()
      resume(Effect.fail(new Error(`child exited before emitting a line (code=${code}, signal=${signal})`)))
    }
    const cleanup = () => {
      stdout.removeListener("data", onData)
      child.removeListener("error", onError)
      child.removeListener("exit", onExit)
    }
    stdout.on("data", onData)
    child.on("error", onError)
    child.on("exit", onExit)
    return Effect.sync(cleanup)
  })

const awaitExit = (child: ChildProcess) =>
  Effect.callback<{ readonly code: number | null; readonly signal: NodeJS.Signals | null }>((resume) => {
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => resume(Effect.succeed({ code, signal }))
    child.once("exit", onExit)
    return Effect.sync(() => child.removeListener("exit", onExit))
  })

describe("CrashDurability", () => {
  it.live("recovers an acknowledged write after the writing process is SIGKILLed", () =>
    Effect.gen(function*() {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => mkdtempSync(join(tmpdir(), "effect-local-crash-"))),
        (dir) => Effect.sync(() => rmSync(dir, { recursive: true, force: true }))
      )
      const databasePath = join(directory, "replica.sqlite")

      const writer = yield* spawnChild(["write", databasePath])
      const ack = yield* Schema.decodeUnknownEffect(AckLineJson)(yield* awaitFirstLine(writer))
      assert.strictEqual(ack.journalMode, "wal")

      yield* Effect.sync(() => writer.kill("SIGKILL"))
      const writerExit = yield* awaitExit(writer)
      assert.strictEqual(writerExit.signal, "SIGKILL")
      assert.isNull(writerExit.code)

      assert.isTrue(
        existsSync(`${databasePath}-wal`),
        "the WAL sidecar must survive the kill; a clean close would have checkpointed it away"
      )

      const reader = yield* spawnChild(["read", databasePath, ack.documentId])
      const recovered = yield* Schema.decodeUnknownEffect(ReadLineJson)(yield* awaitFirstLine(reader))
      yield* awaitExit(reader)

      assert.isTrue(recovered.found)
      assert.strictEqual(recovered.title, ack.title)
    }), 30_000)
})
