import type { Page } from "@playwright/test"
import { expect, test } from "@playwright/test"

/**
 * One browser profile is one device in this example: every worker of the origin shares a single
 * OPFS access-handle pool, so only one user's replica may run in a profile at a time. Two tabs of
 * different users opened at the same instant must not both claim it — the loser has to be told,
 * not left with a replica that can never open its database.
 */
test("two users started at once in one profile: exactly one claims it", async ({ browser }) => {
  const context = await browser.newContext()
  const alice = await context.newPage()
  const bob = await context.newPage()

  await Promise.all([
    alice.goto("/?user=alice"),
    bob.goto("/?user=bob")
  ])

  const countMatching = async (selector: string) => {
    const pages: ReadonlyArray<Page> = [alice, bob]
    const counts = await Promise.all(pages.map((page) => page.locator(selector).count()))
    return counts.filter((count) => count > 0).length
  }

  await expect.poll(() => countMatching(".app .sidebar"), { timeout: 30_000 }).toBe(1)
  expect(await countMatching("text=is still active here")).toBe(1)

  await context.close()
})
