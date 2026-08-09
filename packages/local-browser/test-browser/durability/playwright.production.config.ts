import { defineConfig } from "@playwright/test"

export default defineConfig({
  testDir: "./tests-production",
  timeout: 30_000,
  use: {
    browserName: "chromium"
  }
})
