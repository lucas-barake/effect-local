import { expect, test } from "@playwright/test"

test("runs the built durability proof and serves SQLite WASM", async ({ context, page }) => {
  const wasmResponse = context.waitForEvent("response", {
    predicate: (response) => /\/wa-sqlite-[^/]+\.wasm$/.test(new URL(response.url()).pathname),
    timeout: 20_000
  })

  await page.goto("/")
  await expect(page.locator("#status")).toHaveText("Proof complete", { timeout: 20_000 })

  const wasm = await wasmResponse
  expect(wasm.status()).toBe(200)
  expect(wasm.headers()["content-type"]).toContain("application/wasm")
})
