import { defineConfig } from "@playwright/test"

export default defineConfig({
  testDir: "./tests",
  timeout: 30_000,
  use: {
    baseURL: "http://127.0.0.1:4174",
    browserName: "chromium"
  },
  webServer: {
    command: "pnpm dev",
    port: 4174,
    reuseExistingServer: true
  }
})
