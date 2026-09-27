import { mergeConfig, type Plugin } from "vite"
import baseConfig from "./vite.config.js"

const currentRevision = "const wireProtocolRevision = 1"

const nextWireProtocol: Plugin = {
  name: "e2e-next-wire-protocol",
  enforce: "pre",
  transform(code, id) {
    if (!id.endsWith("/local-browser/src/internal/buildIdentity.ts")) return null
    if (!code.includes(currentRevision)) this.error(`buildIdentity.ts no longer declares ${currentRevision}`)
    return code.replace(currentRevision, "const wireProtocolRevision = 2")
  }
}

export default mergeConfig(baseConfig, {
  base: "/next/",
  build: { outDir: "dist-web/next" },
  plugins: [nextWireProtocol]
})
