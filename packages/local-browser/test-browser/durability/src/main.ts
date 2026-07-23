import { client } from "./client.ts"
import "./style.css"

declare global {
  interface Window {
    stage0: typeof client
  }
}

window.stage0 = client

const diagnostics = new BroadcastChannel("effect-local-stage0-diagnostics")
diagnostics.addEventListener("message", (event) => console.log("stage0:", event.data))

const status = document.querySelector<HTMLElement>("#status")!
const statusDot = document.querySelector<HTMLElement>("#status-dot")!
const commandLabel = document.querySelector<HTMLElement>("#command-id")!
const details = document.querySelector<HTMLElement>("#details")!
const runButton = document.querySelector<HTMLButtonElement>("#run-proof")!
const newButton = document.querySelector<HTMLButtonElement>("#new-command")!

const setProof = (name: string, passed: boolean, text: string) => {
  const proof = document.querySelector<HTMLElement>(`[data-proof="${name}"]`)!
  proof.dataset.state = passed ? "passed" : "failed"
  proof.querySelector("p")!.textContent = text
  proof.querySelector(".proof-mark")!.textContent = passed ? "OK" : "!"
}

const runProof = async () => {
  runButton.disabled = true
  newButton.disabled = true
  status.textContent = "Running durability proof"
  statusDot.dataset.state = "working"

  const commandId = localStorage.getItem("stage0-command-id") ?? crypto.randomUUID()
  localStorage.setItem("stage0-command-id", commandId)
  commandLabel.textContent = commandId

  try {
    const before = await client.inspect(commandId)
    const first = await client.commit({
      commandId,
      documentId: "stage0-document",
      value: `value-${commandId.slice(0, 8)}`
    })
    const duplicate = await client.commit({
      commandId,
      documentId: "stage0-document",
      value: "duplicate-must-not-run"
    })
    const after = await client.inspect(commandId)
    const [pulses, database] = await Promise.all([
      client.heartbeat(8, 60),
      client.stressDatabase(250_000)
    ])
    const overlappingPulses = pulses.filter((pulse) =>
      pulse.emittedAt >= database.startedAt && pulse.emittedAt <= database.finishedAt
    ).length

    setProof(
      "atomicity",
      after.eventCount === 1 && after.processedCount === 1 && after.replyCount === 1,
      `${after.eventCount} event · ${after.replyCount} reply · ${after.processedCount} processed`
    )
    setProof(
      "duplicate",
      JSON.stringify(first) === JSON.stringify(duplicate) && after.eventCount === 1,
      `revision ${duplicate.revision} returned without another event`
    )
    setProof(
      "reload",
      before.eventCount === 1 && before.replyCount === 1,
      before.eventCount === 1 ? "OPFS state found before this run" : "Reload once to verify"
    )
    setProof(
      "stream",
      overlappingPulses >= 3,
      `${overlappingPulses} pulses overlapped database work`
    )

    status.textContent = "Proof complete"
    statusDot.dataset.state = "passed"
    details.textContent = JSON.stringify({ after, before, database, duplicate, first, overlappingPulses }, null, 2)
  } catch (error) {
    status.textContent = "Proof failed"
    statusDot.dataset.state = "failed"
    details.textContent = String(error)
  } finally {
    runButton.disabled = false
    newButton.disabled = false
  }
}

runButton.addEventListener("click", () => void runProof())
newButton.addEventListener("click", () => {
  localStorage.removeItem("stage0-command-id")
  void runProof()
})
window.addEventListener("pagehide", () => {
  diagnostics.close()
  void client.dispose()
}, { once: true })

void runProof()
