import {
  expect,
  expectOnline,
  falseStates,
  incoming,
  openDirectMessage,
  outgoing,
  send,
  sendAll,
  test,
  uniqueText
} from "./fixtures.js"

test("a sent message renders locally at once and reaches the peer", async ({ chat }) => {
  const alice = await chat.signIn("alice")
  const bob = await chat.signIn("bob")
  await openDirectMessage(alice, "bob")
  const text = uniqueText("hello")
  await send(alice, text)
  await expect(outgoing(alice, text)).toBeVisible({ timeout: 1_000 })
  await expect(bob.locator(".conversation", { hasText: "Alice" })).toContainText(text, { timeout: 3_000 })
  await openDirectMessage(bob, "alice")
  await expect(incoming(bob, text)).toBeVisible()
})

test("receipts advance from sent to delivered to read", async ({ chat }) => {
  const alice = await chat.signIn("alice")
  const carol = await chat.signIn("carol")
  await openDirectMessage(alice, "carol")
  const text = uniqueText("receipt")
  await send(alice, text)
  const bubble = outgoing(alice, text)
  await expect(bubble.locator(".tick-delivered, .tick-read")).toBeVisible({ timeout: 3_000 })
  await openDirectMessage(carol, "alice")
  await expect(incoming(carol, text)).toBeVisible()
  await expect(bubble.locator(".tick-read")).toBeVisible({ timeout: 3_000 })
})

test("history renders from the local replica after a reload without the sync server", async ({ chat }) => {
  const alice = await chat.signIn("alice")
  await openDirectMessage(alice, "bob")
  const text = uniqueText("durable")
  await send(alice, text)
  await expect(outgoing(alice, text).locator(".tick-sent, .tick-delivered, .tick-read")).toBeVisible({
    timeout: 3_000
  })
  await alice.routeWebSocket("**/sync", (socket) => socket.close())
  await alice.reload()
  await openDirectMessage(alice, "bob")
  await expect(outgoing(alice, text)).toBeVisible({ timeout: 2_000 })
  await expect(alice.getByRole("status").filter({ hasText: "Offline" })).toBeVisible({ timeout: 5_000 })
})

test("a first sign-in on a new device with history paints no false empty state", async ({ chat }) => {
  const alice = await chat.signIn("alice")
  await openDirectMessage(alice, "bob")
  const text = uniqueText("first device")
  await send(alice, text)
  await expect(outgoing(alice, text).locator(".tick-sent, .tick-delivered, .tick-read")).toBeVisible({
    timeout: 3_000
  })
  const newDevice = await chat.signIn("alice")
  await expect(newDevice.locator(".conversation", { hasText: "Bob" })).toBeVisible({ timeout: 5_000 })
  expect(await falseStates(newDevice)).toEqual([])
})

test("a reload of an account with history paints no false empty, offline, or presence states", async ({ chat }) => {
  const alice = await chat.signIn("alice")
  await chat.signIn("bob")
  await expectOnline(alice, "bob")
  await openDirectMessage(alice, "bob")
  const text = uniqueText("history")
  await send(alice, text)
  await expect(outgoing(alice, text).locator(".tick-sent, .tick-delivered, .tick-read")).toBeVisible({
    timeout: 3_000
  })
  await alice.reload()
  await openDirectMessage(alice, "bob")
  await expect(outgoing(alice, text)).toBeVisible({ timeout: 2_000 })
  await expect(alice.locator(".chat-subtitle")).toHaveText("online", { timeout: 3_000 })
  expect(await falseStates(alice)).toEqual([])
})

test("typing and presence reach the peer", async ({ chat }) => {
  const alice = await chat.signIn("alice")
  const dave = await chat.signIn("dave")
  await openDirectMessage(alice, "dave")
  await openDirectMessage(dave, "alice")
  await expect(alice.locator(".chat-subtitle")).toHaveText("online", { timeout: 3_000 })
  await dave.locator(".chat-input").fill("typing")
  await expect(alice.locator(".chat-subtitle-typing")).toBeVisible({ timeout: 3_000 })
  await send(dave, uniqueText("done"))
  await expect(alice.locator(".chat-subtitle-typing")).toHaveCount(0, { timeout: 3_000 })
})

test("the typing indicator clears shortly after the peer stops typing", async ({ chat }) => {
  const alice = await chat.signIn("alice")
  const dave = await chat.signIn("dave")
  await openDirectMessage(alice, "dave")
  await openDirectMessage(dave, "alice")
  await dave.locator(".chat-input").pressSequentially("still here", { delay: 20 })
  await expect(alice.locator(".chat-subtitle-typing")).toBeVisible({ timeout: 3_000 })
  await expect(alice.locator(".chat-subtitle-typing")).toHaveCount(0, { timeout: 4_500 })
})

test("tabs of one user share one replica and keep working after the leader tab closes", async ({ chat }) => {
  const leader = await chat.signIn("alice")
  await chat.signIn("bob")
  await expectOnline(leader, "bob")
  const follower = await chat.openTab(leader, "alice-second-tab")
  await expect(follower.locator(".sidebar-me")).toHaveText("Alice")
  await openDirectMessage(follower, "bob")
  const text = uniqueText("from the second tab")
  await send(follower, text)
  await expect(outgoing(follower, text)).toBeVisible({ timeout: 1_000 })
  await openDirectMessage(leader, "bob")
  await expect(outgoing(leader, text)).toBeVisible({ timeout: 3_000 })
  await leader.close()
  const after = uniqueText("after the leader closed")
  await send(follower, after)
  await expect(outgoing(follower, after)).toBeVisible({ timeout: 5_000 })
  await expect(outgoing(follower, after).locator(".tick-sent, .tick-delivered, .tick-read")).toBeVisible({
    timeout: 5_000
  })
  expect(await falseStates(follower)).toEqual([])
})

test("a tab from a newer deploy takes over the replica and the older tab asks for a reload", async ({ chat }) => {
  const older = await chat.signIn("alice")
  await chat.signIn("bob")
  await expectOnline(older, "bob")
  await openDirectMessage(older, "bob")
  const text = uniqueText("before the deploy")
  await send(older, text)
  await expect(outgoing(older, text).locator(".tick-sent, .tick-delivered, .tick-read")).toBeVisible({
    timeout: 3_000
  })
  const newer = await chat.openTab(older, "alice-newer-deploy", "/next/")
  await expect(newer.locator(".sidebar-me")).toHaveText("Alice")
  await expect(older.getByRole("status").filter({ hasText: "This app was updated in another tab." })).toBeVisible({
    timeout: 5_000
  })
  await expect(older.getByRole("button", { name: "Reload" })).toBeVisible()
  await openDirectMessage(newer, "bob")
  await expect(outgoing(newer, text)).toBeVisible({ timeout: 5_000 })
  const after = uniqueText("after the deploy")
  await send(newer, after)
  await expect(outgoing(newer, after).locator(".tick-sent, .tick-delivered, .tick-read")).toBeVisible({
    timeout: 5_000
  })
  expect(await falseStates(newer)).toEqual([])
})

test("a wrong or empty password shows the typed login failure", async ({ chat }) => {
  const page = await chat.openLoginPage()
  await page.locator(".login-user", { hasText: "Alice" }).click()
  await page.locator(".login-password").fill("wrong")
  await page.locator(".login-submit").click()
  await expect(page.locator(".login-error")).toHaveText("Invalid username or password")
  await page.locator(".login-password").fill("")
  await page.locator(".login-submit").click()
  await expect(page.locator(".login-error")).toHaveText("Invalid username or password")
})

test("logging out in one tab signs out every tab of the origin", async ({ chat }) => {
  const first = await chat.signIn("carol")
  const second = await chat.openTab(first, "carol-second-tab")
  await expect(second.locator(".sidebar-me")).toHaveText("Carol")
  await first.locator(".sidebar-logout").click()
  await expect(first.locator(".login-submit")).toBeVisible()
  await expect(second.locator(".login-submit")).toBeVisible({ timeout: 3_000 })
  await expect(second.locator(".sidebar-me")).toHaveCount(0)
})

test("loading earlier messages keeps the first visible message in view", async ({ chat }) => {
  const bob = await chat.signIn("bob")
  await openDirectMessage(bob, "carol")
  const texts = Array.from({ length: 56 }, (_, index) => uniqueText(`backlog ${index}`))
  await sendAll(bob, texts)
  await expect(outgoing(bob, texts[texts.length - 1])).toBeVisible()
  const loadEarlier = bob.getByRole("button", { name: "Load earlier messages" })
  await expect(loadEarlier).toBeVisible({ timeout: 3_000 })
  await bob.locator(".chat-messages").evaluate((list) => {
    list.scrollTop = 0
  })
  const firstVisible = bob.locator(".bubble-row").first()
  const anchorText = await firstVisible.locator(".bubble-text").innerText()
  await loadEarlier.click()
  await expect(outgoing(bob, texts[0])).toHaveCount(1)
  await expect(outgoing(bob, anchorText)).toBeInViewport()
  await expect(outgoing(bob, texts[texts.length - 1])).not.toBeInViewport()
})

test("loading earlier messages in the same frame as a scroll keeps the first visible message in view", async ({ chat }) => {
  const bob = await chat.signIn("bob")
  await openDirectMessage(bob, "dave")
  const texts = Array.from({ length: 56 }, (_, index) => uniqueText(`scrolled ${index}`))
  await sendAll(bob, texts)
  await expect(outgoing(bob, texts[texts.length - 1])).toBeVisible()
  await expect(bob.getByRole("button", { name: "Load earlier messages" })).toBeVisible({ timeout: 3_000 })
  const anchorText = await bob.locator(".chat-messages").evaluate((list) => {
    list.scrollTop = 0
    const first = list.querySelector("[data-message-row] .bubble-text")?.textContent ?? ""
    const loadEarlier = list.querySelector(".chat-load-more")
    if (loadEarlier instanceof HTMLButtonElement) loadEarlier.click()
    return first
  })
  await expect(outgoing(bob, texts[0])).toHaveCount(1)
  await expect(outgoing(bob, anchorText)).toBeInViewport()
})

test("a 375 px wide viewport shows one pane at a time with a reachable composer", async ({ chat }) => {
  const dave = await chat.signIn("dave")
  await dave.setViewportSize({ width: 375, height: 740 })
  await openDirectMessage(dave, "bob")
  await expect(dave.locator(".chat-input")).toBeInViewport({ ratio: 1 })
  await expect(dave.getByRole("button", { name: "Send message" })).toBeInViewport({ ratio: 1 })
  const input = await dave.locator(".chat-input").boundingBox()
  expect(input?.width ?? 0).toBeGreaterThan(200)
  await expect(dave.locator(".sidebar-me")).not.toBeInViewport()
  await dave.getByRole("button", { name: "Back to conversations" }).click()
  await expect(dave.locator(".sidebar-me")).toBeInViewport()
  await expect(dave.locator(".chat-input")).toHaveCount(0)
})

test("the chat exposes accessible names, receipt states, and polite announcements", async ({ chat }) => {
  const login = await chat.openLoginPage()
  const aliceButton = login.getByRole("button", { name: "Alice", exact: true })
  await expect(aliceButton).toBeVisible()
  const avatar = await aliceButton.locator(".avatar").boundingBox()
  expect(avatar?.width).toBeGreaterThan(0)
  expect(avatar?.width).toBe(avatar?.height)
  await expect(login.getByLabel("Password")).toBeVisible()

  const alice = await chat.signIn("alice")
  const carol = await chat.signIn("carol")
  await openDirectMessage(alice, "carol")
  await openDirectMessage(carol, "alice")
  await alice.locator(".chat-input").focus()
  const outline = await alice.locator(".chat-input").evaluate((input) => getComputedStyle(input).outlineStyle)
  expect(outline).not.toBe("none")

  const text = uniqueText("announce")
  await alice.locator(".chat-input").fill(text)
  await alice.getByRole("button", { name: "Send message" }).click()
  const bubble = outgoing(alice, text)
  await expect(bubble.getByRole("img", { name: /^(Sending|Sent|Delivered|Read)$/ })).toBeVisible()
  await expect(bubble).toContainText("You")
  await expect(incoming(carol, text)).toContainText("Alice")
  await expect(carol.locator("[aria-live='polite']", { hasText: text })).toHaveCount(1)
  await expect(bubble.getByRole("img", { name: "Read" })).toBeVisible({ timeout: 3_000 })

  await carol.locator(".chat-input").fill("typing")
  await expect(alice.locator("[aria-live='polite']", { hasText: "typing…" })).toHaveCount(1, { timeout: 3_000 })
})
