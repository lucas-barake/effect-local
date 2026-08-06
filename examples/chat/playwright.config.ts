import { defineConfig } from "@playwright/test"
import { fileURLToPath } from "node:url"

const cwd = fileURLToPath(new URL(".", import.meta.url))

/**
 * Requires the docker compose stack (Postgres + Jaeger): `docker compose up -d`. The suite runs
 * its own relay server on its own port against the `chat_test` database, so a dev relay on the
 * default port can keep running.
 *
 * The relay's inbox and the seed archive persist in `chat_test` across runs, so prior-run
 * messages can legitimately replay into the fresh browser contexts. The spec asserts on
 * run-unique message bodies rather than on message counts.
 */
const relayPort = 8788
const appPort = 5176

export default defineConfig({
  testDir: "./tests",
  timeout: 120_000,
  // One worker: every test shares one relay process and one set of user identities.
  workers: 1,
  use: {
    baseURL: `http://127.0.0.1:${appPort}`,
    browserName: "chromium"
  },
  webServer: [
    {
      command: "pnpm exec tsx server/main.ts",
      cwd,
      env: {
        CHAT_RELAY_PORT: String(relayPort),
        DATABASE_URL: "postgres://chat:chat@localhost:5433/chat_test"
      },
      url: `http://127.0.0.1:${relayPort}/ready`,
      reuseExistingServer: false,
      timeout: 180_000
    },
    {
      command: `pnpm exec vite --host 127.0.0.1 --port ${appPort} --strictPort`,
      cwd,
      env: { VITE_RELAY_PORT: String(relayPort) },
      port: appPort,
      reuseExistingServer: false,
      timeout: 180_000
    }
  ]
})
