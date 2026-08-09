import { fileURLToPath } from "node:url"
import { makeViteTest } from "../playwright.ts"

export const { expect, test } = makeViteTest({
  configFile: fileURLToPath(new URL("./vite.config.ts", import.meta.url)),
  mode: "production",
  env: { EFFECT_LOCAL_RUNTIME_GATE: "1" }
})
