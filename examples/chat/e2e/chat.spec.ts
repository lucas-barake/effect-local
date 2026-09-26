import { expect, incoming, openDirectMessage, outgoing, send, test, uniqueText } from "./fixtures.js"

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
})
