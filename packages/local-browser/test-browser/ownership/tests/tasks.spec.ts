import { expect, test } from "@playwright/test"
import type { Page } from "@playwright/test"
import { readFile } from "node:fs/promises"

const ownerInfo = (page: Page) =>
  expect.poll(async () => {
    try {
      return await page.evaluate(() => {
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
    } catch (error) {
      // The offline shell reloads the page once its service worker claims it.
      if (String(error).includes("Execution context was destroyed")) return undefined
      throw error
    }
  }, { timeout: 20_000 }).not.toBeUndefined()

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
  await expect(page.getByText("Local replica ready")).toBeVisible()

  const title = `Offline ${crypto.randomUUID()}`
  const titleInput = page.getByLabel("New task title")
  await titleInput.fill(title)
  await expect(titleInput).toHaveValue(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  await page.reload()
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  await context.setOffline(false)
  await expect(page.getByText("Local replica ready")).toBeVisible()
  await expect(page.getByText(title, { exact: true })).toBeVisible()
})

test("shows degraded status when the replica transport is lost", async ({ page }) => {
  await page.addInitScript(() => {
    const addEventListener = MessagePort.prototype.addEventListener
    MessagePort.prototype.addEventListener = function(type, listener, options) {
      if (type !== "message" || typeof listener !== "function") {
        return addEventListener.call(this, type, listener, options)
      }
      return addEventListener.call(this, type, function(this: MessagePort, event: MessageEvent) {
        const result = listener.call(this, event)
        const body = Array.isArray(event.data) ? event.data[1] : undefined
        const state = globalThis as typeof globalThis & { __effectLocalStatusTransportFailed?: boolean }
        if (
          state.__effectLocalStatusTransportFailed !== true &&
          body?._tag === "Chunk" &&
          body.values?.some((value: { readonly _tag?: string }) => value?._tag === "Ready")
        ) {
          state.__effectLocalStatusTransportFailed = true
          queueMicrotask(() => {
            this.dispatchEvent(new ErrorEvent("error", { message: "forced transport loss" }))
          })
        }
        return result
      }, options)
    } as typeof MessagePort.prototype.addEventListener
  })
  await page.goto("/")
  await expect.poll(() =>
    page.evaluate(() =>
      (globalThis as typeof globalThis & { __effectLocalStatusTransportFailed?: boolean })
        .__effectLocalStatusTransportFailed
    )
  ).toBe(true)
  await expect(page.getByText("Local replica degraded: StorageUnavailable")).toBeVisible({ timeout: 20_000 })
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

  const archivedTitle = `Archived backup ${crypto.randomUUID()}`
  const postBackupTitle = `Post backup ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(archivedTitle)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).toBeVisible()
  await page.getByRole("button", { name: `Mark ${archivedTitle} complete` }).click()
  await page.getByRole("button", { name: "Completed" }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).toBeVisible()

  const downloadPromise = page.waitForEvent("download")
  await page.getByRole("button", { name: "Download" }).click()
  const download = await downloadPromise
  const path = await download.path()
  expect(path).not.toBeNull()
  await expect(page.getByText("Backup downloaded")).toBeVisible()

  await page.getByRole("button", { name: `Delete ${archivedTitle}` }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).not.toBeVisible()
  await page.getByRole("button", { name: "Active" }).click()
  await page.getByLabel("New task title").fill(postBackupTitle)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(postBackupTitle, { exact: true })).toBeVisible()

  page.once("dialog", (dialog) => void dialog.accept())
  await page.getByLabel("Choose backup file").setInputFiles(path!)
  await expect(page.getByText("Backup restored")).toBeVisible({ timeout: 20_000 })
  await expect(page.getByText(postBackupTitle, { exact: true })).not.toBeVisible()
  await page.getByRole("button", { name: "Completed" }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).toBeVisible()

  await page.reload()
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await expect(page.getByText(postBackupTitle, { exact: true })).not.toBeVisible()
  await page.getByRole("button", { name: "Completed" }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).toBeVisible()
})

test("fails safely and recovers after abrupt owner termination during restore", async ({ context, page }) => {
  test.setTimeout(90_000)
  await page.addInitScript(() => {
    const addEventListener = MessagePort.prototype.addEventListener
    MessagePort.prototype.addEventListener = function(type, listener, options) {
      if (type !== "message" || typeof listener !== "function") {
        return addEventListener.call(this, type, listener, options)
      }
      return addEventListener.call(this, type, function(this: MessagePort, event: MessageEvent) {
        const state = globalThis as typeof globalThis & {
          __effectLocalHoldRestorePull?: boolean
          __effectLocalRestorePullCount?: number
          __effectLocalRestorePullHeld?: boolean
        }
        if (state.__effectLocalHoldRestorePull !== true || event.data?._tag !== "Pull") {
          return listener.call(this, event)
        }
        state.__effectLocalRestorePullCount = (state.__effectLocalRestorePullCount ?? 0) + 1
        if (
          state.__effectLocalRestorePullCount === 2 &&
          state.__effectLocalRestorePullHeld !== true
        ) {
          state.__effectLocalRestorePullHeld = true
          return
        }
        return listener.call(this, event)
      }, options)
    } as typeof MessagePort.prototype.addEventListener
  })
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

  const archivedTitle = `Interrupted restore ${crypto.randomUUID()}`
  const postBackupTitle = `Pre-restore state ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(archivedTitle)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).toBeVisible()

  const downloadPromise = page.waitForEvent("download")
  await page.getByRole("button", { name: "Download" }).click()
  const download = await downloadPromise
  const path = await download.path()
  expect(path).not.toBeNull()
  await expect(page.getByText("Backup downloaded")).toBeVisible()
  const backup = await readFile(path!)
  const firstNewline = backup.indexOf(0x0a)
  expect(firstNewline).toBeGreaterThan(0)
  const paddedBackup = Buffer.concat([
    backup.subarray(0, firstNewline),
    Buffer.alloc(128 * 1024, 0x20),
    backup.subarray(firstNewline)
  ])

  await page.getByRole("button", { name: `Delete ${archivedTitle}` }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).not.toBeVisible()
  await page.getByLabel("New task title").fill(postBackupTitle)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(postBackupTitle, { exact: true })).toBeVisible()

  await page.evaluate(() => {
    ;(globalThis as typeof globalThis & { __effectLocalHoldRestorePull?: boolean })
      .__effectLocalHoldRestorePull = true
  })
  page.once("dialog", (dialog) => void dialog.accept())
  await page.getByLabel("Choose backup file").setInputFiles({
    name: "interrupted-restore.ndjson",
    mimeType: "application/x-ndjson",
    buffer: paddedBackup
  })
  await expect.poll(() =>
    page.evaluate(() =>
      (globalThis as typeof globalThis & { __effectLocalRestorePullHeld?: boolean })
        .__effectLocalRestorePullHeld
    )
  ).toBe(true)

  const browser = context.browser()
  expect(browser).not.toBeNull()
  const cdp = await browser!.newBrowserCDPSession()
  const targets = await cdp.send("Target.getTargets")
  const ownerTarget = targets.targetInfos.find((target) =>
    target.type === "shared_worker" && target.url.includes("replica.shared-worker")
  )
  expect(ownerTarget).toBeDefined()
  const closed = await cdp.send("Target.closeTarget", { targetId: ownerTarget!.targetId })
  expect(closed.success).toBe(true)
  await cdp.detach()

  await expect(page.getByRole("alert").first()).toContainText("OperationTimeout", { timeout: 40_000 })
  await expect(page.getByText("Backup restored")).not.toBeVisible()
  await page.close()

  const recoveredPage = await context.newPage()
  await recoveredPage.goto("/")
  await ownerInfo(recoveredPage)
  await expect(recoveredPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  const recoveredOwner = await recoveredPage.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(recoveredOwner.provider).toBe(true)
  expect(recoveredOwner.ownerId).not.toBe(firstOwner.ownerId)
  expect(recoveredOwner.replicaId).toBe(firstOwner.replicaId)
  expect(recoveredOwner.writerGeneration).toBeGreaterThan(firstOwner.writerGeneration)
  await expect(recoveredPage.getByText(archivedTitle, { exact: true })).not.toBeVisible()
  await expect(recoveredPage.getByText(postBackupTitle, { exact: true })).toHaveCount(1)

  const recoveryTitle = `Recovered session ${crypto.randomUUID()}`
  await recoveredPage.getByLabel("New task title").fill(recoveryTitle)
  await recoveredPage.getByRole("button", { name: "Add task" }).click()
  await expect(recoveredPage.getByText(recoveryTitle, { exact: true })).toHaveCount(1)
  await recoveredPage.reload()
  await expect(recoveredPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await expect(recoveredPage.getByText(archivedTitle, { exact: true })).not.toBeVisible()
  await expect(recoveredPage.getByText(postBackupTitle, { exact: true })).toHaveCount(1)
  await expect(recoveredPage.getByText(recoveryTitle, { exact: true })).toHaveCount(1)
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
    replica.port.postMessage({ _tag: "Attach", protocolVersion: 1, rpcPort: rpc.port1 }, [rpc.port1])
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error(`Candidate was not offered provisioning, saw [${candidate.messages.join(", ")}]`)),
        20_000
      )
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

  // The coordinator provisions among every attached tab, so the surviving tab is offered the
  // database first. Exactly one page becomes the provider and both pages converge on the same
  // new owner with an advanced writer generation.
  const readOwnerInfo = (target: typeof attachedPage) =>
    target.evaluate(() =>
      (globalThis as typeof globalThis & {
        readonly __effectLocalOwnerInfo: {
          readonly ownerId: string
          readonly provider: boolean
          readonly replicaId: string
          readonly writerGeneration: number
        }
      }).__effectLocalOwnerInfo
    )
  await expect
    .poll(async () => (await readOwnerInfo(attachedPage))?.ownerId, { timeout: 20_000 })
    .not.toBe(firstOwner.ownerId)
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })

  const [takeoverOwner, survivingOwner] = await Promise.all([
    readOwnerInfo(takeoverPage),
    readOwnerInfo(attachedPage)
  ])
  expect([takeoverOwner.provider, survivingOwner.provider].filter(Boolean)).toHaveLength(1)
  expect(survivingOwner.ownerId).toBe(takeoverOwner.ownerId)
  expect(takeoverOwner.ownerId).not.toBe(firstOwner.ownerId)
  expect(takeoverOwner.replicaId).toBe(firstOwner.replicaId)
  expect(takeoverOwner.writerGeneration).toBeGreaterThan(firstOwner.writerGeneration)
  expect(survivingOwner.writerGeneration).toBe(takeoverOwner.writerGeneration)
  await expect(takeoverPage.getByText(title, { exact: true })).toBeVisible()
  await expect(attachedPage.getByText(title, { exact: true })).toBeVisible()

  const renamed = `${title} renamed`
  await takeoverPage.getByRole("button", { name: `Rename ${title}` }).click()
  await takeoverPage.getByLabel("Task title", { exact: true }).fill(renamed)
  await takeoverPage.getByRole("button", { name: "Save title" }).click()
  await expect(takeoverPage.getByText(renamed, { exact: true })).toBeVisible()

  // A tab that was already attached when the provider died must be handed off to the new owner:
  // it re-attaches on its own and sees every write the new owner commits.
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await expect(attachedPage.getByText(renamed, { exact: true })).toBeVisible({ timeout: 20_000 })
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
