import { expect, test } from "../playwright.production.ts"

test("runs the built durability proof and serves SQLite WASM", async ({ context, page }) => {
  const wasmResponse = context.waitForEvent("response", {
    predicate: (response) => /\/wa-sqlite-[^/]+\.wasm$/.test(new URL(response.url()).pathname),
    timeout: 20_000
  })

  await page.goto("/")
  await expect(page.locator("#status")).toHaveText("Proof failed", { timeout: 20_000 })
  await expect(page.locator("[data-proof=\"reload\"]")).toHaveAttribute("data-state", "failed")
  await page.reload()
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })
  await expect(page.locator("[data-proof=\"atomicity\"]")).toHaveAttribute("data-state", "passed")
  await expect(page.locator("[data-proof=\"duplicate\"]")).toHaveAttribute("data-state", "passed")
  await expect(page.locator("[data-proof=\"reload\"]")).toHaveAttribute("data-state", "passed")
  await expect(page.locator("[data-proof=\"stream\"]")).toHaveAttribute("data-state", "passed")

  const wasm = await wasmResponse
  expect(wasm.status()).toBe(200)
  expect(wasm.headers()["content-type"]).toContain("application/wasm")
})
