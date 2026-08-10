import type { Page } from "@playwright/test"
import { expect, test } from "../playwright.ts"

const openDurabilityProof = async (page: Page) => {
  await page.goto("/")
  await expect(page.locator("#status")).toHaveText("Proof failed", { timeout: 20_000 })
  await expect(page.locator("[data-proof=\"atomicity\"]")).toHaveAttribute("data-state", "passed")
  await expect(page.locator("[data-proof=\"duplicate\"]")).toHaveAttribute("data-state", "passed")
  await expect(page.locator("[data-proof=\"stream\"]")).toHaveAttribute("data-state", "passed")
  await expect(page.locator("[data-proof=\"reload\"]")).toHaveAttribute("data-state", "failed")
  await page.reload()
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })
}

test("persists an entity event and stored reply atomically and deduplicates", async ({ page }) => {
  await openDurabilityProof(page)

  const commandId = crypto.randomUUID()
  const result = await page.evaluate(async (id) => {
    const first = await window.stage0.commit({
      commandId: id,
      documentId: `document-${id}`,
      value: "first"
    })
    const duplicate = await window.stage0.commit({
      commandId: id,
      documentId: `document-${id}`,
      value: "must-not-run"
    })
    const snapshot = await window.stage0.inspect(id)
    return { duplicate, first, snapshot }
  }, commandId)

  expect(result.duplicate).toEqual(result.first)
  expect(result.snapshot).toMatchObject({
    commandId,
    eventCount: 1,
    latestValue: "first",
    processedCount: 1,
    replyCount: 1
  })
  expect(result.snapshot.storedReplyPayload).toContain(commandId)
})

test("rolls back application state and Cluster reply together", async ({ page }) => {
  await openDurabilityProof(page)

  const commandId = crypto.randomUUID()
  const documentId = `rollback-${commandId}`
  await page.evaluate((input) => {
    void window.stage0.rollback({ ...input, value: "must-roll-back" }).catch(() => undefined)
  }, { commandId, documentId })

  try {
    await expect.poll(() =>
      page.evaluate(
        (input) => window.stage0.inspectRollback(input),
        { commandId, documentId }
      )
    ).toEqual({
      commandId,
      eventCount: 0,
      messageCount: 1,
      processedCount: 0,
      replyCount: 0,
      successfulReplyCount: 0,
      triggerCount: 0
    })
  } finally {
    await page.evaluate(
      (input) => window.stage0.cleanupRollback(input),
      { commandId, documentId }
    )
    await expect.poll(() =>
      page.evaluate(
        (input) => window.stage0.inspectRollback(input),
        { commandId, documentId }
      )
    ).toMatchObject({ messageCount: 0, triggerCount: 0 })
  }
})

test("retains application and reply state across reload", async ({ page }) => {
  await openDurabilityProof(page)

  const commandId = crypto.randomUUID()
  const first = await page.evaluate((id) =>
    window.stage0.commit({
      commandId: id,
      documentId: `reload-${id}`,
      value: "survives-reload"
    }), commandId)

  await page.reload()
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })

  const after = await page.evaluate(async (id) => ({
    duplicate: await window.stage0.commit({
      commandId: id,
      documentId: `reload-${id}`,
      value: "must-not-run-after-reload"
    }),
    snapshot: await window.stage0.inspect(id)
  }), commandId)

  expect(after.duplicate).toEqual(first)
  expect(after.snapshot).toMatchObject({
    eventCount: 1,
    latestValue: "survives-reload",
    processedCount: 1,
    replyCount: 1
  })
})

test("streams pulses while a database request remains active", async ({ page }) => {
  await openDurabilityProof(page)

  const result = await page.evaluate(async () => {
    window.stage0.armNextDatabaseResponse()
    let completed = false
    const databasePromise = window.stage0.stressDatabase(250_000).then((database) => {
      completed = true
      return database
    })
    await window.stage0.waitForDatabaseRequest()
    await window.stage0.waitForDatabaseResponse()
    let released = false
    try {
      const pulses = await window.stage0.heartbeat(3)
      const completedWhileHeld = completed
      window.stage0.releaseDatabaseResponse()
      released = true
      return {
        completedWhileHeld,
        database: await databasePromise,
        pulseCount: pulses.length
      }
    } finally {
      if (!released) window.stage0.releaseDatabaseResponse()
    }
  })

  expect(result.completedWhileHeld).toBe(false)
  expect(result.pulseCount).toBe(3)
  expect(result.database.total).toBe(31_250_125_000)
})

test("resumes a suspended workflow after the owner restarts", async ({ page }) => {
  await openDurabilityProof(page)

  const id = crypto.randomUUID()
  const executionId = await page.evaluate((workflowId) => window.stage0.startWorkflow(workflowId), id)
  await expect.poll(() =>
    page.evaluate(
      ({ currentExecutionId, workflowId }) => window.stage0.inspectWorkflow(workflowId, currentExecutionId),
      {
        currentExecutionId: executionId,
        workflowId: id
      }
    )
  ).toMatchObject({ beginCount: 1, completeCount: 0 })

  await page.reload()
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })

  await expect.poll(
    () =>
      page.evaluate(
        ({ currentExecutionId, workflowId }) => window.stage0.inspectWorkflow(workflowId, currentExecutionId),
        {
          currentExecutionId: executionId,
          workflowId: id
        }
      ),
    { timeout: 20_000 }
  ).toMatchObject({
    beginCount: 1,
    completeCount: 1,
    status: "Complete"
  })
})
