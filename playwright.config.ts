import { defineConfig } from "@playwright/test"

export default defineConfig({
  testDir: "./packages/local-browser/test-browser",
  use: { browserName: "chromium" },
  webServer: {
    command: "pnpm --dir examples/browser-spike dev",
    port: 4173,
    reuseExistingServer: true
  }
})
