import { defineConfig } from "@playwright/test"
import { fileURLToPath } from "node:url"

const cwd = fileURLToPath(new URL(".", import.meta.url))

/**
 * Requires the docker compose stack (Postgres + Jaeger): `docker compose up -d`. The suite runs
 * its own relay server on its own port against the `chat_test` database, so a dev relay on the
 * default port can keep running.
 *
 * The relay command resets `chat_test` before booting: without it the retained inbox replays
 * every prior run's traffic into each fresh browser context, and the suite degrades as that
 * backlog grows.
 */
const relayPort = 8788
const appPort = 5176

export default defineConfig({
  testDir: "./tests",
  // The golden path spans first-launch provisioning, several sync round-trips, and heartbeat-paced
  // projection rebuilds on both devices.
  timeout: 240_000,
  // One worker: every test shares one relay process and one set of user identities.
  workers: 1,
  use: {
    baseURL: `http://127.0.0.1:${appPort}`,
    browserName: "chromium"
  },
  webServer: [
    {
      command: "pnpm exec tsx server/reset-test-db.ts && pnpm exec tsx server/main.ts",
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
