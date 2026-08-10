import { BrowserCrypto, BrowserKeyValueStore } from "@effect/platform-browser"
import { client } from "./client.ts"
import "./style.css"
import { Crypto, Effect, Fiber, Layer, Schema } from "effect"
import * as KeyValueStore from "effect/unstable/persistence/KeyValueStore"

declare global {
  interface Window {
    stage0: typeof client
  }
}

window.stage0 = client

const Details = Schema.fromJsonString(Schema.Unknown, { space: 2 })
const BrowserLive = Layer.merge(BrowserCrypto.layer, BrowserKeyValueStore.layerLocalStorage)
const CommandKey = "stage0-command-id"

const diagnostics = new BroadcastChannel("effect-local-stage0-diagnostics")
diagnostics.addEventListener("message", (event) => {
  Effect.runFork(Effect.logDebug("stage0 diagnostics", event.data))
})

const status = document.querySelector<HTMLElement>("#status")!
const statusDot = document.querySelector<HTMLElement>("#status-dot")!
const commandLabel = document.querySelector<HTMLElement>("#command-id")!
const details = document.querySelector<HTMLElement>("#details")!
const runButton = document.querySelector<HTMLButtonElement>("#run-proof")!
const newButton = document.querySelector<HTMLButtonElement>("#new-command")!

const setProof = (name: string, passed: boolean, text: string) => {
  const proof = document.querySelector<HTMLElement>(`[data-proof="${name}"]`)!
  if (passed) {
    proof.dataset.state = "passed"
  } else {
    proof.dataset.state = "failed"
  }
  proof.querySelector("p")!.textContent = text
  if (passed) {
    proof.querySelector(".proof-mark")!.textContent = "OK"
  } else {
    proof.querySelector(".proof-mark")!.textContent = "!"
  }
}

const clientPromise = <A,>(evaluate: () => Promise<A>) => Effect.tryPromise(evaluate)

const proveDatabaseConcurrency = Effect.gen(function*() {
  yield* clientPromise(() => client.armNextDatabaseResponse())
  let completed = false
  const databaseFiber = yield* Effect.forkChild(
    clientPromise(() => client.stressDatabase(250_000)).pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          completed = true
        })
      )
    )
  )
  yield* clientPromise(() => client.waitForDatabaseRequest())
  yield* clientPromise(() => client.waitForDatabaseResponse())
  let released = false
  return yield* Effect.ensuring(
    Effect.gen(function*() {
      const pulses = yield* clientPromise(() => client.heartbeat(3))
      const completedWhileHeld = completed
      yield* clientPromise(() => client.releaseDatabaseResponse())
      released = true
      const database = yield* Fiber.join(databaseFiber)
      return { completedWhileHeld, database, pulseCount: pulses.length }
    }),
    Effect.suspend(() => {
      if (released) return Effect.void
      return clientPromise(() => client.releaseDatabaseResponse()).pipe(Effect.ignore)
    })
  )
})

const runProof = (clearCommand: boolean) => {
  const program = Effect.gen(function*() {
    yield* Effect.sync(() => {
      runButton.disabled = true
      newButton.disabled = true
      status.textContent = "Running durability proof"
      statusDot.dataset.state = "working"
    })

    const store = yield* KeyValueStore.KeyValueStore
    if (clearCommand) {
      yield* store.remove(CommandKey)
    }
    const storedCommandId = yield* store.get(CommandKey)
    let commandId = storedCommandId
    if (commandId === undefined) {
      const crypto = yield* Crypto.Crypto
      commandId = yield* crypto.randomUUIDv4
      yield* store.set(CommandKey, commandId)
    }
    commandLabel.textContent = commandId

    const before = yield* clientPromise(() => client.inspect(commandId))
    const first = yield* clientPromise(() =>
      client.commit({
        commandId,
        documentId: "stage0-document",
        value: `value-${commandId.slice(0, 8)}`
      })
    )
    const duplicate = yield* clientPromise(() =>
      client.commit({
        commandId,
        documentId: "stage0-document",
        value: "duplicate-must-not-run"
      })
    )
    const after = yield* clientPromise(() => client.inspect(commandId))
    const concurrency = yield* proveDatabaseConcurrency
    const firstJson = yield* Schema.encodeEffect(Details)(first)
    const duplicateJson = yield* Schema.encodeEffect(Details)(duplicate)
    const detailsJson = yield* Schema.encodeEffect(Details)({
      after,
      before,
      completedWhileHeld: concurrency.completedWhileHeld,
      database: concurrency.database,
      duplicate,
      first,
      pulseCount: concurrency.pulseCount
    })
    const atomicityPassed = after.eventCount === 1 && after.processedCount === 1 && after.replyCount === 1
    const duplicatePassed = firstJson === duplicateJson && after.eventCount === 1
    const reloadPassed = before.eventCount === 1 && before.replyCount === 1
    const streamPassed = !concurrency.completedWhileHeld && concurrency.pulseCount === 3

    yield* Effect.sync(() => {
      setProof(
        "atomicity",
        atomicityPassed,
        `${after.eventCount} event · ${after.replyCount} reply · ${after.processedCount} processed`
      )
      setProof(
        "duplicate",
        duplicatePassed,
        `revision ${duplicate.revision} returned without another event`
      )
      setProof(
        "stream",
        streamPassed,
        `${concurrency.pulseCount} pulses arrived while the database response was held`
      )

      const passed = atomicityPassed && duplicatePassed && reloadPassed && streamPassed
      if (passed) {
        status.textContent = "Proof complete"
        statusDot.dataset.state = "passed"
      } else {
        status.textContent = "Proof failed"
        statusDot.dataset.state = "failed"
      }
      let reloadDetails = "Reload once to verify"
      if (before.eventCount === 1) {
        reloadDetails = "OPFS state found before this run"
      }
      setProof("reload", reloadPassed, reloadDetails)
      details.textContent = detailsJson
    })
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.sync(() => {
        status.textContent = "Proof failed"
        statusDot.dataset.state = "failed"
        details.textContent = String(cause)
      })
    ),
    Effect.ensuring(Effect.sync(() => {
      runButton.disabled = false
      newButton.disabled = false
    }))
  )

  Effect.runFork(program.pipe(Effect.provide(BrowserLive)))
}

runButton.addEventListener("click", () => {
  runProof(false)
})
newButton.addEventListener("click", () => {
  runProof(true)
})
window.addEventListener("pagehide", () => {
  diagnostics.close()
  Effect.runFork(
    clientPromise(() => client.dispose()).pipe(
      Effect.tapError((cause) => Effect.logError(`Failed to dispose durability client: ${String(cause)}`))
    )
  )
}, { once: true })

runProof(false)
