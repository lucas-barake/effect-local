import { defineConfig } from "@playwright/test"

export default defineConfig({
  testDir: "./tests-production",
  timeout: 60_000,
  use: {
    browserName: "chromium"
  }
})
