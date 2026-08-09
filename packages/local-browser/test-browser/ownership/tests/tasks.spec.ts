import type { Page, Worker as PlaywrightWorker } from "@playwright/test"
import { readFile } from "node:fs/promises"
import { expect, test } from "../playwright.ts"

const ownershipTransitionTimeout = 40_000

const installOwnershipHarness = (
  page: Page,
  options: { readonly armInitially?: boolean; readonly holdProvisionAccepted?: boolean } = {}
) => {
  const timerGateToken = crypto.randomUUID()
  return page.addInitScript(({ armInitially, holdProvisionAccepted, timerGateToken }) => {
    sessionStorage.setItem("effect-local-shell-controlled", "true")
    const signal = () => {
      let resolve!: () => void
      const promise = new Promise<void>((done) => {
        resolve = done
      })
      return { promise, resolve }
    }
    const acceptanceHeld = signal()
    const acceptanceReleased = signal()
    const timerGateHeld = signal()
    const timerGateReleased = signal()
    const timerGate = new BroadcastChannel(`effect-local-ownership-timer-${timerGateToken}`)
    const state: {
      acceptanceHeld: boolean
      armTimerGate: () => void
      releaseAcceptance?: () => void
      releaseTimerGate: () => void
      timerArmed: boolean
      timerGateHeld: boolean
      timerGateReleased: boolean
      readonly waitFor: Record<
        "acceptanceHeld" | "acceptanceReleased" | "timerGateHeld" | "timerGateReleased",
        Promise<void>
      >
      workerUrl: string | undefined
    } = {
      acceptanceHeld: false,
      armTimerGate: () => {
        state.timerArmed = true
        timerGate.postMessage({ _tag: "Arm" })
      },
      releaseTimerGate: () => timerGate.postMessage({ _tag: "Release" }),
      timerArmed: armInitially,
      timerGateHeld: false,
      timerGateReleased: false,
      waitFor: {
        acceptanceHeld: acceptanceHeld.promise,
        acceptanceReleased: acceptanceReleased.promise,
        timerGateHeld: timerGateHeld.promise,
        timerGateReleased: timerGateReleased.promise
      },
      workerUrl: undefined
    }
    Object.defineProperty(globalThis, "__effectLocalOwnershipHarness", { value: state })
    timerGate.addEventListener("message", (event) => {
      if (event.data?._tag === "Held") {
        state.timerGateHeld = true
        timerGateHeld.resolve()
      }
      if (event.data?._tag === "Released") {
        state.timerGateReleased = true
        timerGateReleased.resolve()
      }
    })

    const NativeSharedWorker = globalThis.SharedWorker
    globalThis.SharedWorker = new Proxy(NativeSharedWorker, {
      construct(target, [scriptURL, workerOptions]) {
        const url = new URL(String(scriptURL), globalThis.location.href)
        url.searchParams.set("effectLocalTestTimerGate", timerGateToken)
        if (state.timerArmed) url.searchParams.set("effectLocalTestTimerGateArmed", "true")
        state.workerUrl = url.href
        return Reflect.construct(target, [url, workerOptions])
      }
    })

    if (holdProvisionAccepted) {
      const addEventListener = MessagePort.prototype.addEventListener
      MessagePort.prototype.addEventListener = function(type, listener, listenerOptions) {
        if (type !== "message" || typeof listener !== "function") {
          return addEventListener.call(this, type, listener, listenerOptions)
        }
        return addEventListener.call(this, type, function(this: MessagePort, event: MessageEvent) {
          if (event.data?._tag !== "ProvisionAccepted") return listener.call(this, event)
          state.acceptanceHeld = true
          acceptanceHeld.resolve()
          state.releaseAcceptance = () => {
            state.acceptanceHeld = false
            acceptanceReleased.resolve()
            listener.call(this, event)
          }
        }, listenerOptions)
      } as typeof MessagePort.prototype.addEventListener
    }
  }, {
    armInitially: options.armInitially === true,
    holdProvisionAccepted: options.holdProvisionAccepted === true,
    timerGateToken
  }).then(() => timerGateToken)
}

const waitForHarnessState = (
  page: Page,
  key: "acceptanceHeld" | "acceptanceReleased" | "timerGateHeld" | "timerGateReleased"
) =>
  page.evaluate((key) =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnershipHarness: {
        readonly waitFor: Record<string, Promise<void>>
      }
    }).__effectLocalOwnershipHarness.waitFor[key], key)

const installSharedWorkerTimerUrl = (page: Page, timerGateToken: string) =>
  page.addInitScript((timerGateToken) => {
    const NativeSharedWorker = globalThis.SharedWorker
    globalThis.SharedWorker = new Proxy(NativeSharedWorker, {
      construct(target, [scriptURL, workerOptions]) {
        const url = new URL(String(scriptURL), globalThis.location.href)
        url.searchParams.set("effectLocalTestTimerGate", timerGateToken)
        return Reflect.construct(target, [url, workerOptions])
      }
    })
  }, timerGateToken)

const installExactSharedWorkerUrl = (page: Page, workerUrl: string) =>
  page.addInitScript((workerUrl) => {
    const NativeSharedWorker = globalThis.SharedWorker
    globalThis.SharedWorker = new Proxy(NativeSharedWorker, {
      construct(target, [, workerOptions]) {
        return Reflect.construct(target, [workerUrl, workerOptions])
      }
    })
  }, workerUrl)

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
  }, { timeout: 0 }).not.toBeUndefined()

const installDatabaseBootstrapProbe = (
  page: Page,
  mode: "hold" | "reject"
) => {
  const signal = <A,>() => {
    let resolve!: (value: A) => void
    const promise = new Promise<A>((done) => {
      resolve = done
    })
    return { promise, resolve }
  }
  const error = signal<string>()
  const release = signal<boolean>()
  const startPosted = signal<string>()
  const workersClosed = signal<void>()
  const openWorkers = new Set<PlaywrightWorker>()
  let workerErrored = false
  const resolveWorkersClosed = () => {
    if (workerErrored && openWorkers.size === 0) workersClosed.resolve()
  }
  page.on("worker", (worker) => {
    openWorkers.add(worker)
    worker.once("close", () => {
      openWorkers.delete(worker)
      resolveWorkersClosed()
    })
  })
  return Promise.all([
    page.exposeBinding("__effectLocalDatabaseBootstrapError", (_, message: string) => {
      workerErrored = true
      error.resolve(message)
      resolveWorkersClosed()
    }),
    page.exposeBinding("__effectLocalDatabaseBootstrapRelease", () => release.promise),
    page.exposeBinding("__effectLocalDatabaseBootstrapStarted", (_, url: string) => {
      startPosted.resolve(url)
    })
  ]).then(() =>
    page.addInitScript((mode) => {
      sessionStorage.setItem("effect-local-shell-controlled", "true")
      if (
        mode === "hold" &&
        sessionStorage.getItem("effectLocalDatabaseBootstrapReleased") !== null
      ) return
      const NativeWorker = globalThis.Worker
      const bindings = globalThis as typeof globalThis & {
        readonly __effectLocalDatabaseBootstrapError: (message: string) => Promise<void>
        readonly __effectLocalDatabaseBootstrapRelease: () => Promise<boolean>
        readonly __effectLocalDatabaseBootstrapStarted: (url: string) => Promise<void>
      }
      globalThis.Worker = new Proxy(NativeWorker, {
        construct(target, [scriptURL, options]) {
          const url = new URL(String(scriptURL), globalThis.location.href)
          if (!url.pathname.endsWith("/opfs.worker.ts")) {
            return Reflect.construct(target, [scriptURL, options])
          }
          url.searchParams.set(
            mode === "hold" ? "effectLocalTestHoldImport" : "effectLocalTestRejectImport",
            "true"
          )
          const worker = Reflect.construct(target, [url, options]) as Worker
          worker.addEventListener("error", (event) => {
            void bindings.__effectLocalDatabaseBootstrapError(event.message)
          })
          const postMessage = worker.postMessage
          Object.defineProperty(worker, "postMessage", {
            value(message: unknown, transferOrOptions?: unknown) {
              if (
                typeof message === "object" &&
                message !== null &&
                "_tag" in message &&
                message._tag === "DatabaseWorkerStart"
              ) {
                void bindings.__effectLocalDatabaseBootstrapStarted(url.href)
              }
              Reflect.apply(
                postMessage,
                worker,
                transferOrOptions === undefined ? [message] : [message, transferOrOptions]
              )
            }
          })
          void bindings.__effectLocalDatabaseBootstrapRelease().then((persist) => {
            if (persist) sessionStorage.setItem("effectLocalDatabaseBootstrapReleased", "true")
            worker.postMessage("effectLocalTestReleaseImport")
          }, () => undefined)
          return worker
        }
      })
    }, mode)
  ).then(() => ({
    release: release.resolve,
    waitFor: {
      error: error.promise,
      startPosted: startPosted.promise,
      workersClosed: workersClosed.promise
    }
  }))
}

const waitForDatabaseBootstrap = async (
  probe: Awaited<ReturnType<typeof installDatabaseBootstrapProbe>>
) => expect(await probe.waitFor.startPosted).toContain("type=ignore")

test("buffers database startup while its coordinator module loads", async ({ page }) => {
  test.setTimeout(0)
  const probe = await installDatabaseBootstrapProbe(page, "hold")
  await page.goto("/")
  await waitForDatabaseBootstrap(probe)
  probe.release(true)

  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 0 })
  const title = `Buffered database start ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()
  await page.reload()
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 0 })
  await expect(page.getByText(title, { exact: true })).toBeVisible()
})

test("terminates the database worker when its coordinator module fails to load", async ({ page }) => {
  test.setTimeout(0)
  const probe = await installDatabaseBootstrapProbe(page, "reject")
  await page.goto("/")
  await waitForDatabaseBootstrap(probe)

  probe.release(false)
  expect(await probe.waitFor.error).toContain("effect-local test coordinator import failure")
  await probe.waitFor.workersClosed
})

test("creates, updates, completes, deletes, and reloads local tasks", async ({ page }) => {
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
  await page.getByRole("button", { name: "Completed" }).click()
  await expect(page.getByText(renamed, { exact: true })).toBeVisible()

  await page.getByRole("button", { name: `Delete ${renamed}` }).click()
  await expect(page.getByText(renamed, { exact: true })).not.toBeVisible()
})

test("keeps local writes available while browser networking is offline", async ({ context, page }) => {
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
  await page.evaluate(() => navigator.serviceWorker.ready)
  await expect.poll(() => page.evaluate(() => navigator.serviceWorker.controller !== null)).toBe(true)
  // No assertion on the status text here or after coming back online. This app is a direct replica
  // with no peer and no relay, so nothing it renders is derived from network state, and the same
  // string shows whether or not the context is offline. The contract worth protecting is below: a
  // write lands while offline and survives a reload with no network.
  await context.setOffline(true)

  const title = `Offline ${crypto.randomUUID()}`
  const titleInput = page.getByLabel("New task title")
  await titleInput.fill(title)
  await expect(titleInput).toHaveValue(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  await page.reload()
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  await context.setOffline(false)
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
  await expect(page.getByText("Local replica degraded: StorageUnavailable")).toBeVisible({
    timeout: ownershipTransitionTimeout
  })
})

test("does not persist arbitrary same origin responses in the offline shell cache", async ({ context, page }) => {
  const privateUrl = `/private-api-${crypto.randomUUID()}`
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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

test("downloads and restores a canonical local backup", async ({ page }, testInfo) => {
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })

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
  const backupPath = testInfo.outputPath("backup.ndjson")
  await download.saveAs(backupPath)
  await expect(page.getByText("Backup downloaded")).toBeVisible()

  await page.getByRole("button", { name: `Delete ${archivedTitle}` }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).not.toBeVisible()
  await page.getByRole("button", { name: "Active" }).click()
  await page.getByLabel("New task title").fill(postBackupTitle)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(postBackupTitle, { exact: true })).toBeVisible()

  page.once("dialog", (dialog) => void dialog.accept())
  await page.getByLabel("Choose backup file").setInputFiles(backupPath)
  await expect(page.getByText("Backup restored")).toBeVisible({ timeout: ownershipTransitionTimeout })
  await expect(page.getByText(postBackupTitle, { exact: true })).not.toBeVisible()
  await page.getByRole("button", { name: "Completed" }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).toBeVisible()

  await page.reload()
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
  await expect(page.getByText(postBackupTitle, { exact: true })).not.toBeVisible()
  await page.getByRole("button", { name: "Completed" }).click()
  await expect(page.getByText(archivedTitle, { exact: true })).toBeVisible()
})

test("fails safely and recovers after abrupt owner termination during restore", async ({ context, page }) => {
  test.setTimeout(0)
  await page.addInitScript(() => {
    let resolveRestorePullHeld!: () => void
    const restorePullHeld = new Promise<void>((resolve) => {
      resolveRestorePullHeld = resolve
    })
    Object.defineProperty(globalThis, "__effectLocalRestorePullHeldPromise", { value: restorePullHeld })
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
          resolveRestorePullHeld()
          return
        }
        return listener.call(this, event)
      }, options)
    } as typeof MessagePort.prototype.addEventListener
  })
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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

  await page.clock.install()
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
  await page.evaluate(() =>
    (globalThis as typeof globalThis & { readonly __effectLocalRestorePullHeldPromise: Promise<void> })
      .__effectLocalRestorePullHeldPromise
  )

  const browser = context.browser()
  expect(browser).not.toBeNull()
  const cdp = await browser!.newBrowserCDPSession()
  const targets = await cdp.send("Target.getTargets")
  const ownerTarget = targets.targetInfos.find((target) =>
    target.type === "shared_worker" && target.url.includes("replica.shared-worker")
  )
  expect(ownerTarget).toBeDefined()
  await cdp.send("Target.setDiscoverTargets", { discover: true })
  const ownerDestroyed = new Promise<void>((resolve) => {
    const onDestroyed = (event: { readonly targetId: string }) => {
      if (event.targetId !== ownerTarget!.targetId) return
      cdp.off("Target.targetDestroyed", onDestroyed)
      resolve()
    }
    cdp.on("Target.targetDestroyed", onDestroyed)
  })
  const closed = await cdp.send("Target.closeTarget", { targetId: ownerTarget!.targetId })
  expect(closed.success).toBe(true)
  await ownerDestroyed
  await cdp.detach()

  await page.clock.runFor(30_000)
  await expect(page.getByRole("alert").first()).toContainText("OperationTimeout")
  await expect(page.getByText("Backup restored")).not.toBeVisible()
  await page.close()

  const recoveredPage = await context.newPage()
  await recoveredPage.goto("/")
  await ownerInfo(recoveredPage)
  await expect(recoveredPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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
  await expect(recoveredPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
  await expect(recoveredPage.getByText(archivedTitle, { exact: true })).not.toBeVisible()
  await expect(recoveredPage.getByText(postBackupTitle, { exact: true })).toHaveCount(1)
  await expect(recoveredPage.getByText(recoveryTitle, { exact: true })).toHaveCount(1)
})

test("shares one durable owner across tabs", async ({ context, page }) => {
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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
  test.setTimeout(0)
  const timerGateToken = await installOwnershipHarness(page)
  await page.goto("/")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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
  const workerUrl = await page.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnershipHarness: { readonly workerUrl: string }
    }).__effectLocalOwnershipHarness.workerUrl
  )

  const title = `Expired candidate ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible()

  const candidatePage = await context.newPage()
  await candidatePage.goto("/service-worker.js")
  await candidatePage.evaluate(async ({ timerGateToken, workerUrl }) => {
    const signal = () => {
      let resolve!: () => void
      const promise = new Promise<void>((done) => {
        resolve = done
      })
      return { promise, resolve }
    }
    const provisioned = signal()
    const rejected = signal()
    const timerGateArmed = signal()
    const timerGateHeld = signal()
    const state = globalThis as typeof globalThis & {
      __effectLocalExpiredCandidate?: {
        readonly attach: () => void
        readonly messages: Array<string>
        readonly provisioned: Promise<void>
        readonly rejected: Promise<void>
        readonly replica: SharedWorker
        readonly rpcPort: MessagePort
        readonly timerGate: BroadcastChannel
        readonly timerGateArmed: Promise<void>
        readonly timerGateHeld: Promise<void>
      }
    }
    const timerGate = new BroadcastChannel(`effect-local-ownership-timer-${timerGateToken}`)
    const replica = new SharedWorker(workerUrl, {
      name: "effect-local-tasks",
      type: "module"
    })
    const rpc = new MessageChannel()
    const candidate = {
      attach: () => {
        replica.port.start()
        replica.port.postMessage({ _tag: "Attach", protocolVersion: 1, rpcPort: rpc.port1 }, [rpc.port1])
      },
      messages: [] as Array<string>,
      provisioned: provisioned.promise,
      rejected: rejected.promise,
      replica,
      rpcPort: rpc.port2,
      timerGate,
      timerGateArmed: timerGateArmed.promise,
      timerGateHeld: timerGateHeld.promise
    }
    state.__effectLocalExpiredCandidate = candidate
    replica.port.addEventListener("message", (event) => {
      const message = event.data as { readonly _tag: string }
      candidate.messages.push(message._tag)
      if (message._tag === "Provision") provisioned.resolve()
      if (message._tag === "ProvisionRejected") rejected.resolve()
    })
    timerGate.addEventListener("message", (event) => {
      if (event.data?._tag === "Armed") timerGateArmed.resolve()
      if (event.data?._tag === "Held") timerGateHeld.resolve()
    })
    timerGate.postMessage({ _tag: "Arm" })
  }, { timerGateToken, workerUrl })
  await candidatePage.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalExpiredCandidate: { readonly timerGateArmed: Promise<void> }
    }).__effectLocalExpiredCandidate.timerGateArmed
  )
  await page.close()
  await candidatePage.evaluate(async () => {
    const candidate = (globalThis as typeof globalThis & {
      readonly __effectLocalExpiredCandidate: {
        readonly attach: () => void
        readonly provisioned: Promise<void>
        readonly replica: SharedWorker
      }
    }).__effectLocalExpiredCandidate
    candidate.attach()
    await candidate.provisioned
  })

  await candidatePage.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalExpiredCandidate: { readonly timerGateHeld: Promise<void> }
    }).__effectLocalExpiredCandidate.timerGateHeld
  )
  await candidatePage.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalExpiredCandidate: { readonly timerGate: BroadcastChannel }
    }).__effectLocalExpiredCandidate.timerGate.postMessage({ _tag: "Release" })
  )
  const candidateMessages = await candidatePage.evaluate(async () => {
    const candidate = (globalThis as typeof globalThis & {
      readonly __effectLocalExpiredCandidate: {
        readonly messages: ReadonlyArray<string>
        readonly rejected: Promise<void>
      }
    }).__effectLocalExpiredCandidate
    await candidate.rejected
    return candidate.messages
  })
  expect(candidateMessages).toEqual(["Provision", "ProvisionRejected"])

  const healthyPage = await context.newPage()
  await installSharedWorkerTimerUrl(healthyPage, timerGateToken)
  await healthyPage.goto("/")
  await ownerInfo(healthyPage)
  await expect(healthyPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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
  await installSharedWorkerTimerUrl(attachedPage, timerGateToken)
  await attachedPage.goto("/")
  await ownerInfo(attachedPage)
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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

  await candidatePage.evaluate(() => {
    const state = globalThis as typeof globalThis & {
      __effectLocalExpiredCandidate?: {
        readonly replica: SharedWorker
        readonly rpcPort: MessagePort
        readonly timerGate: BroadcastChannel
      }
    }
    state.__effectLocalExpiredCandidate?.replica.port.close()
    state.__effectLocalExpiredCandidate?.rpcPort.close()
    state.__effectLocalExpiredCandidate?.timerGate.close()
    delete state.__effectLocalExpiredCandidate
  })
  await candidatePage.close()
})

test("reprovisions the durable owner after its database provider dies", async ({ context, page }) => {
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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
  await expect(takeoverPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })

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
    .poll(async () => (await readOwnerInfo(attachedPage))?.ownerId, { timeout: ownershipTransitionTimeout })
    .not.toBe(firstOwner.ownerId)
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })

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
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
  await expect(attachedPage.getByText(renamed, { exact: true })).toBeVisible({ timeout: ownershipTransitionTimeout })
})

test("keeps an accepted database provider while its acknowledgement is delayed", async ({ context, page }) => {
  test.setTimeout(0)
  const timerGateToken = await installOwnershipHarness(page, {
    armInitially: true,
    holdProvisionAccepted: true
  })
  await page.goto("/")
  await waitForHarnessState(page, "acceptanceHeld")
  await ownerInfo(page)
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
  await waitForHarnessState(page, "timerGateHeld")
  const providerBeforeExpiry = await page.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnerInfo: {
        readonly ownerId: string
        readonly provider: boolean
        readonly replicaId: string
        readonly writerGeneration: number
      }
    }).__effectLocalOwnerInfo
  )
  expect(providerBeforeExpiry.provider).toBe(true)
  const workerUrl = await page.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnershipHarness: { readonly workerUrl: string }
    }).__effectLocalOwnershipHarness.workerUrl
  )

  await page.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnershipHarness: { readonly releaseTimerGate: () => void }
    }).__effectLocalOwnershipHarness.releaseTimerGate()
  )
  await waitForHarnessState(page, "timerGateReleased")
  const providerAfterExpiry = await page.evaluate(() =>
    (globalThis as typeof globalThis & { readonly __effectLocalOwnerInfo: unknown }).__effectLocalOwnerInfo
  )
  expect(providerAfterExpiry).toEqual(providerBeforeExpiry)
  await waitForHarnessState(page, "acceptanceHeld")

  await page.evaluate(() =>
    (globalThis as typeof globalThis & {
      readonly __effectLocalOwnershipHarness: { readonly releaseAcceptance: () => void }
    }).__effectLocalOwnershipHarness.releaseAcceptance()
  )
  await waitForHarnessState(page, "acceptanceReleased")

  const attachedPage = await context.newPage()
  await installExactSharedWorkerUrl(attachedPage, workerUrl)
  await attachedPage.goto("/")
  await ownerInfo(attachedPage)
  await expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: ownershipTransitionTimeout })
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
  expect(attachedOwner.ownerId).toBe(providerBeforeExpiry.ownerId)
  expect(attachedOwner.replicaId).toBe(providerBeforeExpiry.replicaId)
  expect(attachedOwner.writerGeneration).toBe(providerBeforeExpiry.writerGeneration)
})
