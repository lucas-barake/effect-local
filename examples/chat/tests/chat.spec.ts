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
  await expect(page.locator(".roster-row:disabled")).toHaveCount(0, { timeout: 60_000 })
  return page
}

test("messages, ticks, read receipts, presence, and multi-tab", async ({ browser }) => {
  const alice = await openUser(browser, "alice")
  const bob = await openUser(browser, "bob")

  const run = Date.now().toString(36)
  const fromAlice = `hello bob ${run}`
  const fromBob = `hello alice ${run}`

  const aliceConversation = alice.getByTestId("conversation-bob")
  const bobConversation = bob.getByTestId("conversation-alice")
  await expect(aliceConversation).toBeEnabled({ timeout: 60_000 })
  await expect(bobConversation).toBeEnabled({ timeout: 60_000 })

  // Socket connectivity precedes pair-session readiness. Presence is the first document exchange
  // in both directions, so it proves the path exercised below is ready instead of racing startup.
  await aliceConversation.click()
  await bobConversation.click()
  await expect(alice.locator(".chat-presence")).toHaveAttribute("data-online", "true", {
    timeout: 90_000
  })
  await expect(bob.locator(".chat-presence")).toHaveAttribute("data-online", "true", {
    timeout: 90_000
  })

  // Alice opens the Bob conversation and sends. The bubble starts on the clock (committed locally)
  // and reaches at least relay custody once the push is accepted.
  await alice.getByPlaceholder("Message Bob").fill(fromAlice)
  await alice.getByRole("button", { name: "Send" }).click()
  // Usually instant (the send itself rebuilds the sender's projections), but replayed inbound
  // traffic arriving in the same window can re-block the list until the next heartbeat.
  const aliceBubble = alice.locator(".bubble", { hasText: fromAlice })
  await expect(aliceBubble).toBeVisible({ timeout: 40_000 })
  await expect(aliceBubble.locator(".tick-wrap")).toHaveAttribute(
    "data-status",
    /Received by server|Delivered|Read/,
    { timeout: 30_000 }
  )

  // Bob's device applies the message and acknowledges it without his tab doing anything; his
  // sidebar shows the conversation with an unread count until he opens it.
  await expect(bobConversation).toContainText(fromAlice, { timeout: 60_000 })

  // Delivered: bob's replica holds the message durably, which his open app reports back into the
  // document whether or not the conversation is on screen.
  await expect(aliceBubble.locator(".tick-wrap")).toHaveAttribute("data-status", /Delivered|Read/, {
    timeout: 60_000
  })

  // Read: bob opens the conversation. Inbound changes can keep the message list blocked until
  // bob's next local command — up to one heartbeat interval — so this allows more than one.
  await bobConversation.click()
  await expect(bob.locator(".bubble", { hasText: fromAlice })).toBeVisible({ timeout: 40_000 })
  await expect(aliceBubble.locator(".tick-wrap")).toHaveAttribute("data-status", "Read", {
    timeout: 60_000
  })

  // The reply flows the other way.
  await bob.getByPlaceholder("Message Alice").fill(fromBob)
  await bob.getByRole("button", { name: "Send" }).click()
  await expect(bob.locator(".bubble", { hasText: fromBob })).toHaveCount(1, { timeout: 40_000 })
  await expect(alice.locator(".bubble", { hasText: fromBob })).toBeVisible({ timeout: 60_000 })

  // Multi-tab: a second tab of the same profile attaches to the same SharedWorker replica and
  // sees the same conversation immediately.
  const aliceTab2 = await alice.context().newPage()
  await aliceTab2.goto(`/?user=alice`)
  await expect(aliceTab2.getByText("connected", { exact: true })).toBeVisible({ timeout: 60_000 })
  const aliceTab2Conversation = aliceTab2.getByTestId("conversation-bob")
  await expect(aliceTab2Conversation).toBeEnabled({ timeout: 60_000 })
  await aliceTab2Conversation.click()
  await expect(aliceTab2.locator(".bubble", { hasText: fromAlice })).toBeVisible({ timeout: 60_000 })
  await expect(aliceTab2.locator(".bubble", { hasText: fromBob })).toBeVisible({ timeout: 60_000 })
})
