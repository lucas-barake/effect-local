import { defineConfig } from "@playwright/test"
import { fileURLToPath } from "node:url"

const cwd = fileURLToPath(new URL("../..", import.meta.url))

export default defineConfig({
  testDir: "./tests",
  // CI runs this several times slower than a laptop, and the exchange itself is polled.
  timeout: 180_000,
  // One worker. Every test in this project shares one relay process and one pair of device
  // identities, so running files in parallel makes them contend for the same inboxes.
  workers: 1,
  use: {
    baseURL: "http://127.0.0.1:4177",
    browserName: "chromium"
  },
  webServer: [
    {
      command: "pnpm exec tsx test-browser/relay/relay-server.ts",
      cwd,
      env: { EFFECT_LOCAL_RELAY_PORT: "4176" },
      url: "http://127.0.0.1:4176/ready",
      // Not reused. The relay holds inbox state for a fixed pair of device principals, so a server
      // left from an earlier run starts this one with that run's retained rows under the same inbox
      // keys. Nothing observed here needed that to go wrong, but a test whose starting state
      // depends on what ran before it is not one whose failures mean anything.
      reuseExistingServer: false
    },
    {
      command: "pnpm exec vite --config test-browser/relay/vite.config.ts --host 127.0.0.1 --port 4177",
      cwd,
      port: 4177,
      reuseExistingServer: true
    }
  ]
})
