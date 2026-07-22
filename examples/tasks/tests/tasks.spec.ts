import { expect, test } from "@playwright/test"
import type { Page } from "@playwright/test"

const ownerInfo = (page: Page) =>
  expect.poll(() =>
    page.evaluate(() => {
      const state = globalThis as typeof globalThis & {
        readonly __effectLocalOwnerError?: string
        readonly __effectLocalOwnerInfo?: {
          readonly ownerId: string
          readonly provider: boolean
          readonly replicaId: string
          readonly writerGeneration: number
        }
      }
      if (state.__effectLocalOwnerError !== undefined) throw new Error(state.__effectLocalOwnerError)
      return state.__effectLocalOwnerInfo
    })
  ).not.toBeUndefined()

test("creates, updates, completes, deletes, and reloads local tasks", async ({ page }) => {
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await expect(page.getByText(/Persistent storage|Best effort storage/)).toBeVisible()

  const title = `Task ${crypto.randomUUID()}`
  const renamed = `${title} renamed`
  await page.getByLabel("New task title").fill(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  await page.getByRole("button", { name: `Rename ${title}` }).click()
  await page.getByLabel("Task title", { exact: true }).fill(renamed)
  await page.getByRole("button", { name: "Save title" }).click()
  await expect(page.getByText(renamed, { exact: true })).toBeVisible()

  await page.getByRole("button", { name: `Mark ${renamed} complete` }).click()
  await page.getByRole("button", { name: "Completed" }).click()
  await expect(page.getByText(renamed, { exact: true })).toBeVisible()

  await page.reload()
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await page.getByRole("button", { name: "Completed" }).click()
  await expect(page.getByText(renamed, { exact: true })).toBeVisible()

  await page.getByRole("button", { name: `Delete ${renamed}` }).click()
  await expect(page.getByText(renamed, { exact: true })).not.toBeVisible()
})

test("keeps local writes available while browser networking is offline", async ({ context, page }) => {
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await page.evaluate(() => navigator.serviceWorker.ready)
  await expect.poll(() => page.evaluate(() => navigator.serviceWorker.controller !== null)).toBe(true)
  await context.setOffline(true)
  await expect(page.getByText("Offline, saved locally")).toBeVisible()

  const title = `Offline ${crypto.randomUUID()}`
  const titleInput = page.getByLabel("New task title")
  await titleInput.fill(title)
  await expect(titleInput).toHaveValue(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  await page.reload()
  await ownerInfo(page)
  await expect(page.getByText("Offline, saved locally")).toBeVisible({ timeout: 20_000 })
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  await context.setOffline(false)
  await expect(page.getByText("Local replica ready")).toBeVisible()
  await expect(page.getByText(title, { exact: true })).toBeVisible()
})

test("does not persist arbitrary same origin responses in the offline shell cache", async ({ context, page }) => {
  const privateUrl = `/private-api-${crypto.randomUUID()}`
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await page.evaluate(() => navigator.serviceWorker.ready)
  await expect.poll(() => page.evaluate(() => navigator.serviceWorker.controller !== null)).toBe(true)
  expect(await page.evaluate((url) => fetch(url).then((response) => response.text()), privateUrl)).toContain(
    "Local Tasks"
  )
  await context.setOffline(true)
  const offlineResponse = await page.evaluate(
    (url) => fetch(url).then((response) => response.text(), () => null),
    privateUrl
  )
  expect(offlineResponse).toBeNull()
})

test("downloads and restores a canonical local backup", async ({ page }) => {
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })

  const title = `Backup ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  const downloadPromise = page.waitForEvent("download")
  await page.getByRole("button", { name: "Download" }).click()
  const download = await downloadPromise
  const path = await download.path()
  expect(path).not.toBeNull()
  await expect(page.getByText("Backup downloaded")).toBeVisible()

  page.once("dialog", (dialog) => void dialog.accept())
  await page.getByLabel("Choose backup file").setInputFiles(path!)
  await expect(page.getByText("Backup restored")).toBeVisible({ timeout: 20_000 })
})

test("shares one durable owner across tabs", async ({ context, page }) => {
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  const firstOwner = await page.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(firstOwner.provider).toBe(true)

  const firstTitle = `First tab ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(firstTitle)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(firstTitle, { exact: true })).toBeVisible()

  const attachedPage = await context.newPage()
  await attachedPage.goto("/")
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await ownerInfo(attachedPage)
  await expect(attachedPage.getByText(firstTitle, { exact: true })).toBeVisible()
  const attachedOwner = await attachedPage.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(attachedOwner.provider).toBe(false)
  expect(attachedOwner.ownerId).toBe(firstOwner.ownerId)
  expect(attachedOwner.replicaId).toBe(firstOwner.replicaId)
  expect(attachedOwner.writerGeneration).toBe(firstOwner.writerGeneration)

  const secondTitle = `Second tab ${crypto.randomUUID()}`
  await attachedPage.getByLabel("New task title").fill(secondTitle)
  await attachedPage.getByRole("button", { name: "Add task" }).click()
  await expect(attachedPage.getByText(secondTitle, { exact: true })).toBeVisible()
})

test("expires a stalled provisioning candidate before assigning a healthy provider", async ({ context, page }) => {
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  const firstOwner = await page.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(firstOwner.provider).toBe(true)

  const title = `Expired candidate ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  const candidatePage = await context.newPage()
  await candidatePage.goto("/service-worker.js")
  await page.close()
  const provisionedAt = await candidatePage.evaluate(async () => {
    const state = globalThis as typeof globalThis & {
      __effectLocalExpiredCandidate?: {
        readonly messages: Array<string>
        provisionedAt: number
        readonly replica: SharedWorker
        readonly rpcPort: MessagePort
      }
    }
    const replica = new SharedWorker("/src/replica.shared-worker.ts?worker_file&type=module", {
      name: "effect-local-tasks",
      type: "module"
    })
    const rpc = new MessageChannel()
    const candidate = { messages: [] as Array<string>, provisionedAt: 0, replica, rpcPort: rpc.port2 }
    state.__effectLocalExpiredCandidate = candidate
    replica.port.start()
    replica.port.postMessage({ _tag: "Attach", rpcPort: rpc.port1 }, [rpc.port1])
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Candidate was not offered provisioning")), 10_000)
      replica.port.addEventListener("message", (event) => {
        const message = event.data as { readonly _tag: string }
        candidate.messages.push(message._tag)
        if (message._tag !== "Provision") return
        candidate.provisionedAt = Date.now()
        clearTimeout(timeout)
        resolve()
      })
    })
    return candidate.provisionedAt
  })

  const healthyPage = await context.newPage()
  await healthyPage.goto("/")
  await ownerInfo(healthyPage)
  await expect(healthyPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  expect(Date.now() - provisionedAt).toBeGreaterThanOrEqual(1_900)
  const healthyOwner = await healthyPage.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(healthyOwner.provider).toBe(true)
  expect(healthyOwner.ownerId).not.toBe(firstOwner.ownerId)
  expect(healthyOwner.replicaId).toBe(firstOwner.replicaId)
  expect(healthyOwner.writerGeneration).toBeGreaterThan(firstOwner.writerGeneration)
  await expect(healthyPage.getByText(title, { exact: true })).toBeVisible()

  const renamed = `${title} renamed`
  await healthyPage.getByRole("button", { name: `Rename ${title}` }).click()
  await healthyPage.getByLabel("Task title", { exact: true }).fill(renamed)
  await healthyPage.getByRole("button", { name: "Save title" }).click()
  await expect(healthyPage.getByText(renamed, { exact: true })).toBeVisible()

  const attachedPage = await context.newPage()
  await attachedPage.goto("/")
  await ownerInfo(attachedPage)
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  const attachedOwner = await attachedPage.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(attachedOwner.provider).toBe(false)
  expect(attachedOwner.ownerId).toBe(healthyOwner.ownerId)
  expect(attachedOwner.replicaId).toBe(healthyOwner.replicaId)
  expect(attachedOwner.writerGeneration).toBe(healthyOwner.writerGeneration)
  await expect(attachedPage.getByText(renamed, { exact: true })).toBeVisible()

  await expect.poll(() =>
    candidatePage.evaluate(() =>
      (globalThis as typeof globalThis & {
        readonly __effectLocalExpiredCandidate: { readonly messages: ReadonlyArray<string> }
      }).__effectLocalExpiredCandidate.messages
    )
  ).toEqual(["Provision", "ProvisionRejected"])
  await candidatePage.evaluate(() => {
    const state = globalThis as typeof globalThis & {
      __effectLocalExpiredCandidate?: {
        readonly replica: SharedWorker
        readonly rpcPort: MessagePort
      }
    }
    state.__effectLocalExpiredCandidate?.replica.port.close()
    state.__effectLocalExpiredCandidate?.rpcPort.close()
    delete state.__effectLocalExpiredCandidate
  })
  await candidatePage.close()
})

test("reprovisions the durable owner after its database provider dies", async ({ context, page }) => {
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await ownerInfo(page)
  const firstOwner = await page.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(firstOwner.provider).toBe(true)

  const title = `Takeover ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  const attachedPage = await context.newPage()
  await attachedPage.goto("/")
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await ownerInfo(attachedPage)
  const attachedOwner = await attachedPage.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(attachedOwner.provider).toBe(false)
  expect(attachedOwner.ownerId).toBe(firstOwner.ownerId)

  await page.close()

  const takeoverPage = await context.newPage()
  await takeoverPage.goto("/")
  await ownerInfo(takeoverPage)
  await expect(takeoverPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  const takeoverOwner = await takeoverPage.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(takeoverOwner.provider).toBe(true)
  expect(takeoverOwner.ownerId).not.toBe(firstOwner.ownerId)
  expect(takeoverOwner.replicaId).toBe(firstOwner.replicaId)
  expect(takeoverOwner.writerGeneration).toBeGreaterThan(firstOwner.writerGeneration)
  await expect(takeoverPage.getByText(title, { exact: true })).toBeVisible()

  const renamed = `${title} renamed`
  await takeoverPage.getByRole("button", { name: `Rename ${title}` }).click()
  await takeoverPage.getByLabel("Task title", { exact: true }).fill(renamed)
  await takeoverPage.getByRole("button", { name: "Save title" }).click()
  await expect(takeoverPage.getByText(renamed, { exact: true })).toBeVisible()
})

test("keeps an accepted database provider while its acknowledgement is delayed", async ({ context, page }) => {
  await page.addInitScript(() => {
    const addEventListener = MessagePort.prototype.addEventListener
    MessagePort.prototype.addEventListener = function(type, listener, options) {
      if (type !== "message" || typeof listener !== "function") {
        return addEventListener.call(this, type, listener, options)
      }
      return addEventListener.call(this, type, (event: MessageEvent) => {
        if (event.data?._tag !== "ProvisionAccepted") {
          listener.call(this, event)
          return
        }
        setTimeout(() => {
          listener.call(this, event)
          ;(globalThis as typeof globalThis & { __effectLocalDelayedAcceptanceDelivered?: boolean })
            .__effectLocalDelayedAcceptanceDelivered = true
        }, 3_500)
      }, options)
    } as typeof MessagePort.prototype.addEventListener
  })
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await expect.poll(() =>
    page.evaluate(() =>
      (globalThis as typeof globalThis & { __effectLocalDelayedAcceptanceDelivered?: boolean })
        .__effectLocalDelayedAcceptanceDelivered
    )
  ).toBe(true)

  const attachedPage = await context.newPage()
  await attachedPage.goto("/")
  await ownerInfo(attachedPage)
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
})
