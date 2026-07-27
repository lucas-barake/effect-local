import { defineConfig } from "@playwright/test"
import { fileURLToPath } from "node:url"

const cwd = fileURLToPath(new URL("../..", import.meta.url))

export default defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  // One worker. Every test in this project shares one relay process and one pair of device
  // identities, so running files in parallel makes them contend for the same inboxes.
  workers: 1,
  use: {
    baseURL: "http://127.0.0.1:4177",
    browserName: "chromium"
  },
  // Two servers: the page, and a real relay for it to talk to. The relay is an ordinary Node
  // process composed from this package's own building blocks, which is the point of the fixture.
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
