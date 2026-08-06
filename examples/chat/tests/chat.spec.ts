import type { Browser, Page } from "@playwright/test"
import { expect, test } from "@playwright/test"

/**
 * Two Chromium contexts rather than two tabs: tabs of one context share storage, and a browser
 * profile is one device in this example's model. Nothing is stubbed on either side of the wire —
 * both pages run the full SharedWorker + OPFS + relay stack against the real server.
 */

const openUser = async (browser: Browser, user: string): Promise<Page> => {
  const context = await browser.newContext()
  const page = await context.newPage()
  await page.goto(`/?user=${user}`)
  await expect(page.getByText("connected", { exact: true })).toBeVisible({ timeout: 60_000 })
  return page
}

test("messages, ticks, read receipts, presence, and multi-tab", async ({ browser }) => {
  const alice = await openUser(browser, "alice")
  const bob = await openUser(browser, "bob")

  const run = Date.now().toString(36)
  const fromAlice = `hello bob ${run}`
  const fromBob = `hello alice ${run}`

  // Alice opens the Bob conversation and sends. The bubble starts on the clock (committed locally)
  // and reaches at least relay custody once the push is accepted.
  await alice.getByTestId("conversation-bob").click()
  await alice.getByPlaceholder("Message Bob").fill(fromAlice)
  await alice.getByRole("button", { name: "Send" }).click()
  const aliceBubble = alice.locator(".bubble", { hasText: fromAlice })
  await expect(aliceBubble).toBeVisible()
  await expect(aliceBubble.locator(".tick-wrap")).toHaveAttribute(
    "data-status",
    /Received by server|Delivered|Read/,
    { timeout: 30_000 }
  )

  // Bob's device applies the message and acknowledges it without his tab doing anything; his
  // sidebar shows the conversation with an unread count until he opens it.
  await expect(bob.getByTestId("conversation-alice")).toContainText(fromAlice, { timeout: 60_000 })

  // Delivered: bob's replica holds the message durably, which his open app reports back into the
  // document whether or not the conversation is on screen.
  await expect(aliceBubble.locator(".tick-wrap")).toHaveAttribute("data-status", /Delivered|Read/, {
    timeout: 60_000
  })

  // Read: bob opens the conversation.
  await bob.getByTestId("conversation-alice").click()
  await expect(bob.locator(".bubble", { hasText: fromAlice })).toBeVisible()
  await expect(aliceBubble.locator(".tick-wrap")).toHaveAttribute("data-status", "Read", {
    timeout: 60_000
  })

  // Presence: both apps are open and heartbeating, so each sees the other online.
  await expect(alice.locator(".chat-presence")).toHaveAttribute("data-online", "true", {
    timeout: 90_000
  })

  // The reply flows the other way.
  await bob.getByPlaceholder("Message Alice").fill(fromBob)
  await bob.getByRole("button", { name: "Send" }).click()
  await expect(bob.locator(".bubble", { hasText: fromBob })).toHaveCount(1)
  await expect(alice.locator(".bubble", { hasText: fromBob })).toBeVisible({ timeout: 60_000 })

  // Multi-tab: a second tab of the same profile attaches to the same SharedWorker replica and
  // sees the same conversation immediately.
  const aliceTab2 = await alice.context().newPage()
  await aliceTab2.goto(`/?user=alice`)
  await aliceTab2.getByTestId("conversation-bob").click()
  await expect(aliceTab2.locator(".bubble", { hasText: fromAlice })).toBeVisible({ timeout: 30_000 })
  await expect(aliceTab2.locator(".bubble", { hasText: fromBob })).toBeVisible({ timeout: 30_000 })
})
