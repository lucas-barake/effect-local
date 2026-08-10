import { client } from "./client.ts"
import "./style.css"
import { Effect, Schema } from "effect"

declare global {
  interface Window {
    stage0: typeof client
  }
}

window.stage0 = client

const Details = Schema.fromJsonString(Schema.Unknown, { space: 2 })

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

const proveDatabaseConcurrency = () => {
  client.armNextDatabaseResponse()
  let completed = false
  const databasePromise = client.stressDatabase(250_000).then((result) => {
    completed = true
    return result
  })
  return client.waitForDatabaseRequest()
    .then(() => client.waitForDatabaseResponse())
    .then(() => client.heartbeat(3))
    .then((pulses) => ({ completedWhileHeld: completed, databasePromise, pulses }))
    .finally(() => client.releaseDatabaseResponse())
}

const runProof = () => {
  runButton.disabled = true
  newButton.disabled = true
  status.textContent = "Running durability proof"
  statusDot.dataset.state = "working"

  const storage = globalThis.localStorage
  const commandId = storage.getItem("stage0-command-id") ?? globalThis.crypto.randomUUID()
  storage.setItem("stage0-command-id", commandId)
  commandLabel.textContent = commandId

  return client.inspect(commandId)
    .then((before) =>
      client.commit({
        commandId,
        documentId: "stage0-document",
        value: `value-${commandId.slice(0, 8)}`
      }).then((first) =>
        client.commit({
          commandId,
          documentId: "stage0-document",
          value: "duplicate-must-not-run"
        }).then((duplicate) => client.inspect(commandId).then((after) => ({ after, before, duplicate, first })))
      )
    )
    .then(({ after, before, duplicate, first }) =>
      proveDatabaseConcurrency().then((concurrency) =>
        concurrency.databasePromise.then((database) => ({ after, before, concurrency, database, duplicate, first }))
      )
    )
    .then(({ after, before, concurrency, database, duplicate, first }) => {
      const atomicityPassed = after.eventCount === 1 && after.processedCount === 1 && after.replyCount === 1
      const duplicatePassed = Schema.encodeSync(Details)(first) === Schema.encodeSync(Details)(duplicate) &&
        after.eventCount === 1
      const reloadPassed = before.eventCount === 1 && before.replyCount === 1
      const streamPassed = !concurrency.completedWhileHeld && concurrency.pulses.length === 3

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
        `${concurrency.pulses.length} pulses arrived while the database response was held`
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
      details.textContent = Schema.encodeSync(Details)({
        after,
        before,
        completedWhileHeld: concurrency.completedWhileHeld,
        database,
        duplicate,
        first,
        pulseCount: concurrency.pulses.length
      })
    })
    .catch((error) => {
      status.textContent = "Proof failed"
      statusDot.dataset.state = "failed"
      details.textContent = String(error)
    })
    .finally(() => {
      runButton.disabled = false
      newButton.disabled = false
    })
}

runButton.addEventListener("click", () => void runProof())
newButton.addEventListener("click", () => {
  globalThis.localStorage.removeItem("stage0-command-id")
  void runProof()
})
window.addEventListener("pagehide", () => {
  diagnostics.close()
  void client.dispose()
}, { once: true })

void runProof()
