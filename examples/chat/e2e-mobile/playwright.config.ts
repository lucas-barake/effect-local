import { defineConfig, devices } from "@playwright/test"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const serverPort = 4199
const clientPort = 5199
const temporaryRoot = tmpdir()
const databaseDirectory = mkdtempSync(join(temporaryRoot, "effect-local-chat-mobile-e2e-"))
const databaseFile = join(databaseDirectory, "chat.db")

export default defineConfig({
  testDir: ".",
  testMatch: "*.spec.ts",
  fullyParallel: false,
  workers: 1,
  forbidOnly: true,
  retries: 0,
  timeout: 900_000,
  expect: { timeout: 30_000 },
  reporter: [["list"]],
  use: {
    baseURL: `http://localhost:${clientPort}`,
    trace: "retain-on-failure"
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: [
    {
      command: "pnpm exec tsx src/main.ts",
      cwd: "../server",
      port: serverPort,
      reuseExistingServer: false,
      env: { CHAT_PORT: String(serverPort), CHAT_DB: databaseFile },
      stdout: "pipe"
    },
    {
      command: `pnpm exec vite build && pnpm exec vite preview --port ${clientPort} --strictPort`,
      cwd: "../client",
      port: clientPort,
      reuseExistingServer: false,
      timeout: 180_000,
      env: { CHAT_SERVER_URL: `http://localhost:${serverPort}` }
    }
  ]
})
