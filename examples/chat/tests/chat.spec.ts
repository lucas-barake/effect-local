import type { BrowserContext, Page, TestInfo } from "@playwright/test"
import { expect, test } from "@playwright/test"

interface FailureCapture {
  readonly attach: () => Promise<void>
  readonly expectClean: () => Promise<void>
}

const installFailureCapture = async (
  page: Page,
  label: string,
  testInfo: TestInfo
): Promise<FailureCapture> => {
  const errors: Array<string> = []

  const recordConsoleError = (source: string, type: string, text: string) => {
    if (type === "error") errors.push(`${source}: ${text}`)
  }

  page.on("pageerror", (error) => errors.push(`page-or-dedicated-worker: ${error.stack ?? error.message}`))
  page.on("console", (message) => recordConsoleError("page-console", message.type(), message.text()))
  page.context().on("console", (message) => {
    if (message.page() === null) recordConsoleError("shared-worker-console", message.type(), message.text())
  })
  page.on("worker", (worker) => {
    worker.on(
      "console",
      (message) => recordConsoleError(`dedicated-worker-console ${worker.url()}`, message.type(), message.text())
    )
  })

  await page.exposeFunction("__captureChatOwnerErrorForTest", (message: string) => {
    errors.push(`shared-worker-owner: ${message}`)
  })
  await page.addInitScript(() => {
    let ownerError: string | undefined
    Object.defineProperty(window, "__chatOwnerError", {
      configurable: true,
      get: () => ownerError,
      set: (message: string | undefined) => {
        ownerError = message
        if (message !== undefined) {
          void (window as unknown as {
            __captureChatOwnerErrorForTest: (value: string) => Promise<void>
          }).__captureChatOwnerErrorForTest(message)
        }
      }
    })
  })

  let attached = false
  const attach = async () => {
    if (attached) return
    attached = true
    await testInfo.attach(`${label}-browser-errors`, {
      body: Buffer.from(errors.length === 0 ? "none\n" : `${errors.join("\n\n")}\n`),
      contentType: "text/plain"
    })
  }

  return {
    attach,
    expectClean: async () => {
      await attach()
      expect(
        errors,
        `${label} emitted unexpected page, worker, or console errors`
      ).toEqual([])
    }
  }
}

const openUserPage = async (
  context: BrowserContext,
  user: string,
  label: string,
  testInfo: TestInfo,
  captures: Array<FailureCapture>
): Promise<Page> => {
  const page = await context.newPage()
  captures.push(await installFailureCapture(page, label, testInfo))
  await page.goto(`/?user=${user}`)
  await expect(page.getByText("connected", { exact: true })).toBeVisible({ timeout: 60_000 })
  await expect(page.locator(".roster-row:disabled")).toHaveCount(0, { timeout: 60_000 })
  await expect.poll(() => page.evaluate(() => window.__chatOwnerInfo)).not.toBeUndefined()
  return page
}

const expectEveryConversationEnabled = async (page: Page) => {
  await expect(page.locator(".roster-row")).toHaveCount(3)
  await expect(page.locator(".roster-row:disabled")).toHaveCount(0)
}

const waitForPresenceOrOwnerError = async (page: Page) => {
  await page.waitForFunction(
    () =>
      window.__chatOwnerError !== undefined ||
      document.querySelector<HTMLElement>(".chat-presence")?.dataset.online === "true",
    undefined,
    {
      timeout: 90_000
    }
  )
  const ownerError = await page.evaluate(() => window.__chatOwnerError)
  if (ownerError !== undefined) throw new Error(ownerError)
}

interface RelayControl {
  readonly disconnect: (pages: ReadonlyArray<Page>) => Promise<void>
  readonly reconnect: () => Promise<void>
}

const setRelayAvailability = async (state: "offline" | "online") => {
  const response = await fetch(`http://127.0.0.1:8788/test/relay/${state}`, { method: "POST" })
  expect(response.ok, `relay test control rejected ${state}`).toBe(true)
}

const controlRelay = (): RelayControl => {
  return {
    disconnect: async (pages) => {
      await setRelayAvailability("offline")
      await Promise.all(
        pages.map((page) =>
          expect(page.locator(".relay-status")).toHaveAttribute("data-status", /Connecting|Disconnected/, {
            timeout: 30_000
          })
        )
      )
    },
    reconnect: async () => {
      await setRelayAvailability("online")
    }
  }
}

const closeWithEvidence = async (
  context: BrowserContext,
  captures: ReadonlyArray<FailureCapture>
) => {
  await setRelayAvailability("online").catch(() => undefined)
  await Promise.all(captures.map((capture) => capture.attach()))
  await context.close()
}

test("all locally ready conversations stay enabled while the relay is unavailable", async ({ browser }, testInfo) => {
  const context = await browser.newContext()
  const captures: Array<FailureCapture> = []
  try {
    const relay = controlRelay()
    const alice = await openUserPage(context, "alice", "alice", testInfo, captures)

    await relay.disconnect([alice])
    await expectEveryConversationEnabled(alice)
    const bob = alice.getByTestId("conversation-bob")
    const carol = alice.getByTestId("conversation-carol")
    const dave = alice.getByTestId("conversation-dave")
    await bob.click()
    await expect(bob).toHaveAttribute("data-selected", "true")
    await carol.click()
    await expect(carol).toHaveAttribute("data-selected", "true")
    await dave.click()
    await expect(dave).toHaveAttribute("data-selected", "true")

    await relay.reconnect()
    await Promise.all(captures.map((capture) => capture.expectClean()))
  } finally {
    await closeWithEvidence(context, captures)
  }
})

test(
  "an offline local send survives reload and reconnects exactly once to an open peer",
  async ({ browser }, testInfo) => {
    const aliceContext = await browser.newContext()
    const bobContext = await browser.newContext()
    const captures: Array<FailureCapture> = []
    try {
      const aliceRelay = controlRelay()
      const alice = await openUserPage(aliceContext, "alice", "alice", testInfo, captures)
      const bob = await openUserPage(bobContext, "bob", "bob", testInfo, captures)
      const aliceConversation = alice.getByTestId("conversation-bob")
      const bobConversation = bob.getByTestId("conversation-alice")
      await aliceConversation.click()
      await bobConversation.click()
      await Promise.all([alice, bob].map(waitForPresenceOrOwnerError))

      const body = `offline alice to bob ${crypto.randomUUID()}`
      await aliceRelay.disconnect([alice, bob])
      await expectEveryConversationEnabled(alice)
      await alice.getByPlaceholder("Message Bob").fill(body)
      await alice.getByRole("button", { name: "Send" }).click()
      await expect(alice.getByRole("alert")).toHaveCount(0)
      await expect(alice.locator(".bubble", { hasText: body })).toHaveCount(1, { timeout: 10_000 })
      await expect(bob.locator(".bubble", { hasText: body })).toHaveCount(0)

      await alice.reload()
      await expectEveryConversationEnabled(alice)
      await alice.getByTestId("conversation-bob").click()
      await expect(alice.locator(".bubble", { hasText: body })).toHaveCount(1, { timeout: 10_000 })
      await expect(bob.locator(".bubble", { hasText: body })).toHaveCount(0)

      await aliceRelay.reconnect()
      await Promise.all(
        [alice, bob].map((page) =>
          expect(page.locator(".relay-status")).toHaveAttribute("data-status", "Connected", { timeout: 60_000 })
        )
      )
      await bob.waitForFunction(
        (messageBody) =>
          window.__chatOwnerError !== undefined ||
          [...document.querySelectorAll(".bubble")].some((bubble) => bubble.textContent?.includes(messageBody)),
        body,
        {
          timeout: 30_000
        }
      )
      const reconnectError = await bob.evaluate(() => window.__chatOwnerError)
      if (reconnectError !== undefined) throw new Error(reconnectError)
      await expect(bob.locator(".bubble", { hasText: body })).toHaveCount(1, { timeout: 30_000 })
      await expect(bobConversation).toContainText(body)
      await expect(alice.locator(".bubble", { hasText: body })).toHaveCount(1)

      await Promise.all(captures.map((capture) => capture.expectClean()))
    } finally {
      await Promise.all([
        closeWithEvidence(aliceContext, captures),
        bobContext.close()
      ])
    }
  }
)

test(
  "presence and typing travel over the transient channel and leave no durable trace",
  async ({ browser }, testInfo) => {
    const aliceContext = await browser.newContext()
    const bobContext = await browser.newContext()
    const captures: Array<FailureCapture> = []
    try {
      const alice = await openUserPage(aliceContext, "alice", "alice", testInfo, captures)
      const bob = await openUserPage(bobContext, "bob", "bob", testInfo, captures)
      await alice.getByTestId("conversation-bob").click()
      await bob.getByTestId("conversation-alice").click()
      await Promise.all([alice, bob].map(waitForPresenceOrOwnerError))

      // Typing is published from the composer, never from a mutation, so nothing is sent.
      await alice.getByPlaceholder("Message Bob").fill("still deciding what to say")
      await expect(bob.locator(".chat-presence")).toHaveText("typing…", { timeout: 20_000 })
      await expect(bob.getByTestId("conversation-alice")).toContainText("typing…")

      // The beacon only beats while a draft exists, so the counterpart's window expires on its own.
      await alice.getByPlaceholder("Message Bob").fill("")
      await expect(bob.locator(".chat-presence")).toHaveText("online", { timeout: 20_000 })
      await expect(bob.locator(".bubble")).toHaveCount(0)

      // A transient value has no outbox and no replay, so a closed device simply goes dark.
      await Promise.all(captures.map((capture) => capture.attach()))
      await aliceContext.close()
      await expect(bob.locator(".chat-presence")).toHaveText("offline", { timeout: 30_000 })
      await expect(bob.locator(".bubble")).toHaveCount(0)

      await Promise.all(captures.map((capture) => capture.expectClean()))
    } finally {
      await Promise.all([
        aliceContext.close(),
        closeWithEvidence(bobContext, captures)
      ])
    }
  }
)

test("two already open tabs observe one new local commit", async ({ browser }, testInfo) => {
  const context = await browser.newContext()
  const captures: Array<FailureCapture> = []
  try {
    const relay = controlRelay()
    const aliceTab1 = await openUserPage(context, "alice", "alice-tab-1", testInfo, captures)
    const aliceTab2 = await openUserPage(context, "alice", "alice-tab-2", testInfo, captures)
    await aliceTab1.getByTestId("conversation-bob").click()
    await aliceTab2.getByTestId("conversation-bob").click()

    const body = `shared alice commit ${crypto.randomUUID()}`
    await relay.disconnect([aliceTab1, aliceTab2])
    await aliceTab1.getByPlaceholder("Message Bob").fill(body)
    await aliceTab1.getByRole("button", { name: "Send" }).click()

    await expect(aliceTab1.locator(".bubble", { hasText: body })).toHaveCount(1, { timeout: 10_000 })
    await expect(aliceTab2.locator(".bubble", { hasText: body })).toHaveCount(1, { timeout: 10_000 })
    await relay.reconnect()
    await Promise.all(captures.map((capture) => capture.expectClean()))
  } finally {
    await closeWithEvidence(context, captures)
  }
})
