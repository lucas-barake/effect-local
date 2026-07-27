import type { DeviceApi } from "./device.ts"
import { start } from "./device.ts"

declare global {
  interface Window {
    relayFixture?: DeviceApi
    relayFixtureError?: string
  }
}

const name = new URL(window.location.href).searchParams.get("device") ?? "alpha"

try {
  window.relayFixture = start(name)
  document.body.dataset.ready = "true"
} catch (cause) {
  window.relayFixtureError = String(cause)
  document.body.dataset.ready = "failed"
}
