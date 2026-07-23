import { defineConfig } from "@playwright/test"
import { fileURLToPath } from "node:url"

export default defineConfig({
  testDir: "./tests",
  timeout: 30_000,
  use: {
    baseURL: "http://127.0.0.1:4173",
    browserName: "chromium"
  },
  webServer: {
    command: "pnpm exec vite --config test-browser/durability/vite.config.ts --host 127.0.0.1 --port 4173",
    cwd: fileURLToPath(new URL("../..", import.meta.url)),
    port: 4173,
    reuseExistingServer: true
  }
})
