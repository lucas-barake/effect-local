import { defineConfig } from "@playwright/test"
import { fileURLToPath } from "node:url"

const cwd = fileURLToPath(new URL("../..", import.meta.url))

// One source of truth for the relay port. The browser reads it as `VITE_RELAY_PORT`, which Vite
// copies out of its own process env for any `VITE_` prefixed key, so it has to be handed to the
// Vite server and not only to the relay. Left to two defaults that merely happen to match, moving
// the relay silently points the browser at whatever else is listening on the old port.
const relayPort = 4176
const appPort = 4177

export default defineConfig({
  testDir: "./tests",
  // CI runs this several times slower than a laptop, and the exchange itself is polled.
  timeout: 180_000,
  // One worker. Every test in this project shares one relay process and one pair of device
  // identities, so running files in parallel makes them contend for the same inboxes.
  workers: 1,
  use: {
    baseURL: `http://127.0.0.1:${appPort}`,
    browserName: "chromium"
  },
  webServer: [
    {
      command: "pnpm exec tsx test-browser/relay/relay-server.ts",
      cwd,
      env: { EFFECT_LOCAL_RELAY_PORT: String(relayPort) },
      url: `http://127.0.0.1:${relayPort}/ready`,
      // Not reused. The relay holds inbox state for a fixed pair of device principals, so a server
      // left from an earlier run starts this one with that run's retained rows under the same inbox
      // keys. Nothing observed here needed that to go wrong, but a test whose starting state
      // depends on what ran before it is not one whose failures mean anything.
      reuseExistingServer: false
    },
    {
      command: `pnpm exec vite --config test-browser/relay/vite.config.ts --host 127.0.0.1 --port ${appPort}`,
      cwd,
      env: { VITE_RELAY_PORT: String(relayPort) },
      port: appPort,
      // Matches the relay rather than reusing. The two servers are one fixture: reusing an app
      // server whose relay was just replaced pairs this run's relay with another checkout's bundle,
      // which is exactly the mismatch a fresh relay was meant to rule out.
      reuseExistingServer: false
    }
  ]
})
