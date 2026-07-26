import { expect, test } from "@playwright/test"
import type { Page } from "@playwright/test"

interface OwnerInfo {
  readonly ownerId: string
  readonly provider: boolean
  readonly replicaId: string
  readonly writerGeneration: number
}

const readOwnerOptional = (page: Page) =>
  page.evaluate(() => {
    const state = globalThis as typeof globalThis & {
      readonly __effectLocalOwnerError?: string
      readonly __effectLocalOwnerInfo?: OwnerInfo
    }
    if (state.__effectLocalOwnerError !== undefined) throw new Error(state.__effectLocalOwnerError)
    return state.__effectLocalOwnerInfo
  }).catch((error: unknown) => {
    // A takeover reload transiently destroys the page's execution context.
    if (String(error).includes("Execution context was destroyed")) return undefined
    throw error
  })

const ownerInfo = (page: Page) =>
  expect.poll(() => readOwnerOptional(page), { timeout: 20_000 }).not.toBeUndefined()

const readOwner = async (page: Page): Promise<OwnerInfo> => {
  const info = await readOwnerOptional(page)
  if (info === undefined) throw new Error("Owner info was not available")
  return info
}

const sessionCount = (page: Page) =>
  page.evaluate(() => (globalThis as typeof globalThis & { __effectLocalSessionCount: () => Promise<number> })
    .__effectLocalSessionCount())

test("hands off an already-attached tab to the new owner after takeover", async ({ context, page }) => {
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  const firstOwner = await readOwner(page)
  expect(firstOwner.provider).toBe(true)

  const title = `Handoff ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  const attachedPage = await context.newPage()
  await attachedPage.goto("/")
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await ownerInfo(attachedPage)
  const attachedOwner = await readOwner(attachedPage)
  expect(attachedOwner.provider).toBe(false)
  expect(attachedOwner.ownerId).toBe(firstOwner.ownerId)
  await expect(attachedPage.getByText(title, { exact: true })).toBeVisible()

  await page.close()

  const takeoverPage = await context.newPage()
  await takeoverPage.goto("/")
  await ownerInfo(takeoverPage)
  await expect(takeoverPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  const takeoverOwner = await readOwner(takeoverPage)
  expect(takeoverOwner.ownerId).not.toBe(firstOwner.ownerId)
  expect(takeoverOwner.replicaId).toBe(firstOwner.replicaId)
  expect(takeoverOwner.writerGeneration).toBeGreaterThan(firstOwner.writerGeneration)

  await expect.poll(async () => (await readOwnerOptional(attachedPage))?.ownerId, { timeout: 20_000 })
    .toBe(takeoverOwner.ownerId)
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await expect(attachedPage.getByText(title, { exact: true })).toBeVisible()

  const renamed = `${title} via attached`
  await attachedPage.getByRole("button", { name: `Rename ${title}` }).click()
  await attachedPage.getByLabel("Task title", { exact: true }).fill(renamed)
  await attachedPage.getByRole("button", { name: "Save title" }).click()
  await expect(attachedPage.getByText(renamed, { exact: true })).toBeVisible()
})

test("recovers when the database worker behind a live provider tab dies", async ({ context, page }) => {
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  const firstOwner = await readOwner(page)
  expect(firstOwner.provider).toBe(true)

  const title = `Storage ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  await page.evaluate(() =>
    (globalThis as typeof globalThis & { __effectLocalTerminateDatabase: () => void }).__effectLocalTerminateDatabase()
  )

  const attachedPage = await context.newPage()
  await attachedPage.goto("/")
  await ownerInfo(attachedPage)
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await expect(attachedPage.getByText(title, { exact: true })).toBeVisible()
})

test("closes a departing tab's session without waiting for lease expiry", async ({ context, page }) => {
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })

  const attachedPage = await context.newPage()
  await attachedPage.goto("/")
  await ownerInfo(attachedPage)
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })

  await expect.poll(() => sessionCount(page), { timeout: 10_000 }).toBe(2)

  await attachedPage.close()

  await expect.poll(() => sessionCount(page), { timeout: 10_000 }).toBe(1)
})
