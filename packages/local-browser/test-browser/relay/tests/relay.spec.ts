import type { Browser, Page } from "@playwright/test"
import { expect, test } from "@playwright/test"

/**
 * Two Chromium contexts rather than two tabs: tabs of one context share storage, which would make
 * them one device. Nothing is stubbed on either side of the wire.
 */

const openDevice = async (browser: Browser, device: "alpha" | "beta"): Promise<Page> => {
  const context = await browser.newContext()
  const page = await context.newPage()
  await page.goto(`/?device=${device}`)
  await page.waitForFunction(() => window.relayFixture !== undefined, undefined, { timeout: 30_000 })
  return page
}

/** Sorted because the merge order of two concurrent list insertions is Automerge's choice. */
const readLabels = (page: Page, documentId: string) =>
  page.evaluate(async (id) => (await window.relayFixture!.readTask(id)).labels, documentId)
    .then((labels) => labels.toSorted())

test("carries a change between two browsers through a real relay", async ({ browser }) => {
  const alpha = await openDevice(browser, "alpha")
  const beta = await openDevice(browser, "beta")
  await expect.poll(() => alpha.locator("body").getAttribute("data-connection-status")).toBe("Disconnected")

  // A document created independently on both devices would be two lineages; this syncs two copies
  // of one.
  const documentId = await alpha.evaluate(() => window.relayFixture!.createTask("shared task"))
  await expect.poll(() => alpha.locator("body").getAttribute("data-connection-status"), { timeout: 30_000 })
    .toBe("Connected")
  const backup = await alpha.evaluate(() => window.relayFixture!.exportBackup())
  await beta.evaluate(async (bytes) => {
    await window.relayFixture!.restoreBackup(bytes)
  }, backup)
  await beta.evaluate((id) => window.relayFixture!.adoptTask(id), documentId)

  await alpha.evaluate((id) => window.relayFixture!.addLabel({ documentId: id, label: "from-alpha" }), documentId)
  await beta.evaluate((id) => window.relayFixture!.addLabel({ documentId: id, label: "from-beta" }), documentId)

  expect(await readLabels(alpha, documentId)).toEqual(["from-alpha"])
  expect(await readLabels(beta, documentId)).toEqual(["from-beta"])

  await alpha.evaluate((id) => window.relayFixture!.push(id), documentId)
  await beta.evaluate((id) => window.relayFixture!.push(id), documentId)

  await expect
    .poll(() => readLabels(alpha, documentId), { timeout: 30_000 })
    .toEqual(["from-alpha", "from-beta"])
  await expect
    .poll(() => readLabels(beta, documentId), { timeout: 30_000 })
    .toEqual(["from-alpha", "from-beta"])

  // Last, because it opens a second session on alpha. A session covers the set of documents this
  // device syncs, so creating another task has to add to that set. Replacing it drops the task
  // above, and the very next push for that task is refused as an unselected document.
  //
  // Appended to this test rather than given its own: the project runs one relay process and one
  // fixed pair of device identities under `workers: 1`, so a second `test()` would start with the
  // inbox rows this one left behind.
  await alpha.evaluate(() => window.relayFixture!.createTask("another task"))
  await alpha.evaluate((id) => window.relayFixture!.push(id), documentId)
})
