import { defineConfig } from "@playwright/test"

export default defineConfig({
  testDir: "./tests-production",
  timeout: 30_000,
  use: {
    baseURL: "http://127.0.0.1:4175",
    browserName: "chromium"
  },
  webServer: {
    command:
      "pnpm exec vite build --config test-browser/ownership/vite.config.ts --outDir .vite/production && pnpm exec vite preview --config test-browser/ownership/vite.config.ts --outDir .vite/production --host 127.0.0.1 --port 4175 --strictPort",
    cwd: "../..",
    env: { EFFECT_LOCAL_RUNTIME_GATE: "1" },
    port: 4175,
    reuseExistingServer: false
  }
})
