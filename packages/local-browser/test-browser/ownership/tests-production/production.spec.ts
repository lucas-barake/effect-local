import { expect, test } from "@playwright/test"
import type { BrowserContext, Page } from "@playwright/test"

const instrumentWorkerBoundaries = (context: BrowserContext) =>
  context.addInitScript(() => {
    const state = globalThis as typeof globalThis & {
      __effectLocalAttachPosted?: boolean
      __effectLocalDedicatedWorkerStarts?: number
      __effectLocalReplicaPorts?: Array<MessagePort>
      __effectLocalSharedWorkers?: Array<SharedWorker>
    }

    const postMessage = MessagePort.prototype.postMessage
    MessagePort.prototype.postMessage = function(message: unknown) {
      if ((message as { readonly _tag?: string } | null)?._tag === "Attach") {
        state.__effectLocalAttachPosted = true
      }
      return Reflect.apply(postMessage, this, arguments)
    } as typeof postMessage

    const NativeWorker = globalThis.Worker
    globalThis.Worker = new Proxy(NativeWorker, {
      construct(target, args, newTarget) {
        state.__effectLocalDedicatedWorkerStarts = (state.__effectLocalDedicatedWorkerStarts ?? 0) + 1
        return Reflect.construct(target, args, newTarget)
      }
    })

    const metricsKey = "__effectLocalSharedWorkerMetrics"
    const readMetrics = () =>
      JSON.parse(sessionStorage.getItem(metricsKey) ?? "{\"created\":0,\"closes\":{}}") as {
        readonly created: number
        readonly closes: Record<string, number>
      }
    const writeMetrics = (metrics: {
      readonly created: number
      readonly closes: Record<string, number>
    }) => sessionStorage.setItem(metricsKey, JSON.stringify(metrics))

    const NativeSharedWorker = globalThis.SharedWorker
    globalThis.SharedWorker = new Proxy(NativeSharedWorker, {
      construct(target, args, newTarget) {
        const worker = Reflect.construct(target, args, newTarget) as SharedWorker
        const metrics = readMetrics()
        const id = String(metrics.created)
        writeMetrics({
          created: metrics.created + 1,
          closes: { ...metrics.closes, [id]: 0 }
        })
        ;(state.__effectLocalReplicaPorts ??= []).push(worker.port)
        ;(state.__effectLocalSharedWorkers ??= []).push(worker)
        const close = worker.port.close.bind(worker.port)
        worker.port.close = () => {
          const current = readMetrics()
          writeMetrics({
            ...current,
            closes: { ...current.closes, [id]: current.closes[id] + 1 }
          })
          close()
        }
        return worker
      }
    })
  })

const attachPosted = (page: Page) =>
  expect.poll(async () => {
    try {
      return await page.evaluate(() =>
        (globalThis as typeof globalThis & { readonly __effectLocalAttachPosted?: boolean })
          .__effectLocalAttachPosted
      )
    } catch (error) {
      if (String(error).includes("Execution context was destroyed")) return false
      throw error
    }
  }, { timeout: 20_000 }).toBe(true)

const ownerInfo = (page: Page) =>
  expect.poll(async () => {
    try {
      return await page.evaluate(() =>
        (globalThis as typeof globalThis & {
          readonly __effectLocalOwnerInfo?: {
            readonly ownerId: string
            readonly provider: boolean
            readonly replicaId: string
            readonly writerGeneration: number
          }
        }).__effectLocalOwnerInfo
      )
    } catch (error) {
      if (String(error).includes("Execution context was destroyed")) return undefined
      throw error
    }
  }, { timeout: 20_000 }).not.toBeUndefined()

const ownerError = (page: Page) =>
  expect.poll(async () => {
    try {
      return await page.evaluate(() =>
        (globalThis as typeof globalThis & { readonly __effectLocalOwnerError?: string })
          .__effectLocalOwnerError
      )
    } catch (error) {
      if (String(error).includes("Execution context was destroyed")) return undefined
      throw error
    }
  }, { timeout: 20_000 }).toBeTruthy()

test("starts concurrent built ownership clients and serves SQLite WASM", async ({ context, page, request }) => {
  await instrumentWorkerBoundaries(context)
  const attachedPage = await context.newPage()
  await request.get("/__effect-local-runtime/hold")
  const wasmResponse = context.waitForEvent("response", {
    predicate: (response) => /\/wa-sqlite-[^/]+\.wasm$/.test(new URL(response.url()).pathname),
    timeout: 20_000
  })

  await Promise.all([page.goto("/"), attachedPage.goto("/")])
  await Promise.all([attachPosted(page), attachPosted(attachedPage)])
  await request.get("/__effect-local-runtime/release")

  await Promise.all([
    expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 }),
    expect(attachedPage.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 }),
    ownerInfo(page),
    ownerInfo(attachedPage)
  ])

  const [first, second, wasm] = await Promise.all([
    page.evaluate(() =>
      (globalThis as typeof globalThis & {
        readonly __effectLocalOwnerInfo: {
          readonly ownerId: string
          readonly provider: boolean
          readonly replicaId: string
          readonly writerGeneration: number
        }
      }).__effectLocalOwnerInfo
    ),
    attachedPage.evaluate(() =>
      (globalThis as typeof globalThis & {
        readonly __effectLocalOwnerInfo: {
          readonly ownerId: string
          readonly provider: boolean
          readonly replicaId: string
          readonly writerGeneration: number
        }
      }).__effectLocalOwnerInfo
    ),
    wasmResponse
  ])

  expect([first.provider, second.provider].filter(Boolean)).toHaveLength(1)
  expect(second.ownerId).toBe(first.ownerId)
  expect(second.replicaId).toBe(first.replicaId)
  expect(second.writerGeneration).toBe(first.writerGeneration)
  expect(wasm.status()).toBe(200)
  expect(wasm.headers()["content-type"]).toContain("application/wasm")
})

test("surfaces a built ownership runtime load failure", async ({ context, page, request }) => {
  await instrumentWorkerBoundaries(context)
  const attachedPage = await context.newPage()

  await request.get("/__effect-local-runtime/hold")
  await Promise.all([page.goto("/"), attachedPage.goto("/")])
  await Promise.all([attachPosted(page), attachPosted(attachedPage)])
  await request.get("/__effect-local-runtime/fail")
  await Promise.all([ownerError(page), ownerError(attachedPage)])

  await Promise.all([
    expect(page.getByRole("alert")).toBeVisible({ timeout: 20_000 }),
    expect(attachedPage.getByRole("alert")).toBeVisible({ timeout: 20_000 }),
    expect(page.getByText("Opening local database")).not.toBeVisible(),
    expect(attachedPage.getByText("Opening local database")).not.toBeVisible()
  ])

  expect(
    await Promise.all([
      page.evaluate(() =>
        (globalThis as typeof globalThis & { readonly __effectLocalDedicatedWorkerStarts?: number })
          .__effectLocalDedicatedWorkerStarts ?? 0
      ),
      attachedPage.evaluate(() =>
        (globalThis as typeof globalThis & { readonly __effectLocalDedicatedWorkerStarts?: number })
          .__effectLocalDedicatedWorkerStarts ?? 0
      )
    ])
  ).toEqual([0, 0])
})

test("does not retain failed post-ready connections across retries", async ({ context, page, request }) => {
  await instrumentWorkerBoundaries(context)
  await request.get("/__effect-local-runtime/release")
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })

  const beforeFailure = await page.evaluate(() => {
    const metrics = JSON.parse(sessionStorage.getItem("__effectLocalSharedWorkerMetrics")!) as {
      readonly created: number
      readonly closes: Record<string, number>
    }
    return {
      created: metrics.created,
      failedId: String(metrics.created - 1)
    }
  })
  await page.evaluate(() => {
    const state = globalThis as typeof globalThis & {
      __effectLocalOwnerInfo?: unknown
      readonly __effectLocalReplicaPorts: Array<MessagePort>
    }
    const ports = state.__effectLocalReplicaPorts
    state.__effectLocalOwnerInfo = undefined
    ports[ports.length - 1].dispatchEvent(
      new MessageEvent("message", {
        data: { _tag: "OwnerError", message: "forced post-ready owner failure" }
      })
    )
  })

  await expect.poll(() =>
    page.evaluate(() => {
      const metrics = JSON.parse(sessionStorage.getItem("__effectLocalSharedWorkerMetrics")!) as {
        readonly created: number
      }
      return metrics.created
    }), { timeout: 20_000 }).toBeGreaterThan(beforeFailure.created)
  await expect.poll(() =>
    page.evaluate((failedId) => {
      const metrics = JSON.parse(sessionStorage.getItem("__effectLocalSharedWorkerMetrics")!) as {
        readonly closes: Record<string, number>
      }
      return metrics.closes[failedId]
    }, beforeFailure.failedId)
  ).toBe(1)
  await ownerInfo(page)

  const title = `Retry ${crypto.randomUUID()}`
  await page.getByLabel("New task title").fill(title)
  await page.getByRole("button", { name: "Add task" }).click()
  await expect(page.getByText(title, { exact: true })).toBeVisible({ timeout: 20_000 })

  await page.evaluate(() => window.dispatchEvent(new PageTransitionEvent("pagehide")))
  const metrics = await page.evaluate(() =>
    JSON.parse(sessionStorage.getItem("__effectLocalSharedWorkerMetrics")!) as {
      readonly created: number
      readonly closes: Record<string, number>
    }
  )
  expect(Object.values(metrics.closes).every((count) => count === 1)).toBe(true)
})

test("does not create retry workers after page teardown", async ({ context, page, request }) => {
  await page.clock.install({ time: new Date("2026-01-01T00:00:00Z") })
  await instrumentWorkerBoundaries(context)
  await request.get("/__effect-local-runtime/release")
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })
  await page.clock.pauseAt(new Date("2026-01-01T00:01:00Z"))

  const beforeFailure = await page.evaluate(() => {
    const metrics = JSON.parse(sessionStorage.getItem("__effectLocalSharedWorkerMetrics")!) as {
      readonly created: number
    }
    return {
      created: metrics.created,
      failedId: String(metrics.created - 1)
    }
  })
  await page.evaluate(() => {
    const ports = (globalThis as typeof globalThis & {
      readonly __effectLocalReplicaPorts: Array<MessagePort>
    }).__effectLocalReplicaPorts
    ports[ports.length - 1].dispatchEvent(
      new MessageEvent("message", {
        data: { _tag: "OwnerError", message: "forced owner failure before teardown" }
      })
    )
    window.dispatchEvent(new PageTransitionEvent("pagehide"))
  })
  await expect(page.locator("#root")).toBeEmpty()
  await page.clock.runFor(1_500)
  const metrics = await page.evaluate(() =>
    JSON.parse(sessionStorage.getItem("__effectLocalSharedWorkerMetrics")!) as {
      readonly created: number
      readonly closes: Record<string, number>
    }
  )
  expect(metrics.created).toBe(beforeFailure.created)
  expect(metrics.closes[beforeFailure.failedId]).toBe(1)
})

test("ignores worker errors queued across page teardown", async ({ context, page, request }) => {
  await instrumentWorkerBoundaries(context)
  await request.get("/__effect-local-runtime/release")
  await page.goto("/")
  await expect(page.getByText("Local replica ready")).toBeVisible({ timeout: 20_000 })

  const workerId = await page.evaluate(() => {
    const metrics = JSON.parse(sessionStorage.getItem("__effectLocalSharedWorkerMetrics")!) as {
      readonly created: number
    }
    return String(metrics.created - 1)
  })
  await page.evaluate(() => {
    const workers = (globalThis as typeof globalThis & {
      readonly __effectLocalSharedWorkers: Array<SharedWorker>
    }).__effectLocalSharedWorkers
    window.dispatchEvent(new PageTransitionEvent("pagehide"))
    workers[workers.length - 1].dispatchEvent(
      new ErrorEvent("error", { error: new Error("queued teardown error"), message: "queued teardown error" })
    )
  })

  const closes = await page.evaluate(() => {
    const metrics = JSON.parse(sessionStorage.getItem("__effectLocalSharedWorkerMetrics")!) as {
      readonly closes: Record<string, number>
    }
    return metrics.closes
  })
  expect(closes[workerId]).toBe(1)
})
