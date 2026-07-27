import type { Browser, Page } from "@playwright/test"
import { expect, test } from "@playwright/test"

/**
 * A change made in one browser reaching another browser through a real relay.
 *
 * Everything here is real: two Chromium contexts with separate OPFS databases, each running the
 * documented relay client composition over a real WebSocket, and a relay process built from this
 * package's own front door, entity and store. Nothing is stubbed on either side of the wire.
 *
 * Two contexts rather than two tabs, because tabs of one context share storage and a SharedWorker,
 * which would make them one device rather than two.
 */

const openDevice = async (browser: Browser, device: "alpha" | "beta"): Promise<Page> => {
  const context = await browser.newContext()
  const page = await context.newPage()
  await page.goto(`/?device=${device}`)
  // Polled rather than asserted once: a cold dev server can still re-optimize and reload the page
  // out from under the first assertion, and the fixture is ready when its api exists, not when a
  // particular load of the document said so.
  await page.waitForFunction(() => window.relayFixture !== undefined, undefined, { timeout: 30_000 })
  return page
}

/**
 * Sorted, because the merge order of two concurrent list insertions is Automerge's to choose rather
 * than this test's. Comparing unsorted passes or fails depending on which insertion Automerge puts
 * first, which reads as a flaky relay and is nothing of the kind.
 */
const readLabels = (page: Page, documentId: string) =>
  page.evaluate(async (id) => (await window.relayFixture!.readTask(id)).labels, documentId)
    .then((labels) => labels.toSorted())

test("carries a change between two browsers through a real relay", async ({ browser }) => {
  const alpha = await openDevice(browser, "alpha")
  const beta = await openDevice(browser, "beta")

  // Alpha owns the document first, then beta clones it. A document created independently on both
  // devices would be two lineages; what is being synced here is two copies of one.
  const documentId = await alpha.evaluate(() => window.relayFixture!.createTask("shared task"))
  const backup = await alpha.evaluate(() => window.relayFixture!.exportBackup())
  await beta.evaluate((bytes) => window.relayFixture!.restoreBackup(bytes), backup)

  // One label on each side, so convergence below is evidence of both directions rather than of one.
  await alpha.evaluate((id) => window.relayFixture!.addLabel(id, "from-alpha"), documentId)
  await beta.evaluate((id) => window.relayFixture!.addLabel(id, "from-beta"), documentId)

  expect(await readLabels(alpha, documentId)).toEqual(["from-alpha"])
  expect(await readLabels(beta, documentId)).toEqual(["from-beta"])

  await beta.evaluate((id) => window.relayFixture!.connect(id), documentId)
  await alpha.evaluate((id) => window.relayFixture!.connect(id), documentId)
  await alpha.evaluate((id) => window.relayFixture!.push(id), documentId)
  await beta.evaluate((id) => window.relayFixture!.push(id), documentId)

  // The whole point: each browser ends up holding the other's label. That can only have arrived
  // over the WebSocket, through the relay's durable inbox, and back out through the recipient's
  // own session.
  await expect
    .poll(() => readLabels(alpha, documentId), { timeout: 30_000 })
    .toEqual(["from-alpha", "from-beta"])
  await expect
    .poll(() => readLabels(beta, documentId), { timeout: 30_000 })
    .toEqual(["from-alpha", "from-beta"])
})
