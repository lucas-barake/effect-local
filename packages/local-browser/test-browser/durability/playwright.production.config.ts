import { defineConfig } from "@playwright/test"

export default defineConfig({
  testDir: "./tests-production",
  timeout: 30_000,
  use: {
    baseURL: "http://127.0.0.1:4176",
    browserName: "chromium"
  },
  webServer: {
    command:
      "pnpm exec vite build --config test-browser/durability/vite.config.ts --outDir .vite/production && pnpm exec vite preview --config test-browser/durability/vite.config.ts --outDir .vite/production --host 127.0.0.1 --port 4176 --strictPort",
    cwd: "../..",
    port: 4176,
    reuseExistingServer: false
  }
})
