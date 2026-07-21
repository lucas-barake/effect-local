import { expect, test } from "@playwright/test"

test("persists an entity event and stored reply atomically and deduplicates", async ({ page }) => {
  await page.goto("/")
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })

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
  await page.goto("/")
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })

  const commandId = crypto.randomUUID()
  const documentId = `rollback-${commandId}`
  await page.evaluate(({ commandId, documentId }) => {
    void window.stage0.rollback({ commandId, documentId, value: "must-roll-back" }).catch(() => undefined)
  }, { commandId, documentId })

  try {
    await expect.poll(() =>
      page.evaluate(
        ({ commandId, documentId }) => window.stage0.inspectRollback({ commandId, documentId }),
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
      ({ commandId, documentId }) => window.stage0.cleanupRollback({ commandId, documentId }),
      { commandId, documentId }
    )
    await expect.poll(() =>
      page.evaluate(
        ({ commandId, documentId }) => window.stage0.inspectRollback({ commandId, documentId }),
        { commandId, documentId }
      )
    ).toMatchObject({ messageCount: 0, triggerCount: 0 })
  }
})

test("retains application and reply state across reload", async ({ page }) => {
  await page.goto("/")
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })

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
  await page.goto("/")
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })

  const result = await page.evaluate(async () => {
    const [pulses, database] = await Promise.all([
      window.stage0.heartbeat(8, 50),
      window.stage0.stressDatabase(250_000)
    ])
    return {
      database,
      pulseCount: pulses.length,
      overlapping: pulses.filter((pulse) =>
        pulse.emittedAt >= database.startedAt && pulse.emittedAt <= database.finishedAt
      ).length
    }
  })

  expect(result.pulseCount).toBe(8)
  expect(result.overlapping).toBeGreaterThanOrEqual(3)
  expect(result.database.total).toBe(31_250_125_000)
})

test("resumes a suspended workflow after the owner restarts", async ({ page }) => {
  await page.goto("/")
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })

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
