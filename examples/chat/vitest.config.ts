import { fileURLToPath } from "node:url"
import { defineConfig } from "vitest/config"

const fixture = (path: string) => fileURLToPath(new URL(path, import.meta.url))

export default defineConfig({
  resolve: {
    alias: {
      "expo": fixture("../../packages/local-expo/test/fixtures/expo.ts"),
      "expo-modules-core": fixture("../../packages/local-expo/test/fixtures/expoModulesCore.ts"),
      "expo-asset": fixture("../../packages/local-expo/test/fixtures/expoAsset.ts"),
      "react-native": fixture("../../packages/local-expo/test/fixtures/reactNative.ts")
    }
  },
  test: {
    include: ["test/**/*.test.ts"],
    setupFiles: ["test/fixtures/expoSecureStore.ts"],
    server: { deps: { inline: ["expo-sqlite", "expo-crypto", "expo-secure-store"] } }
  }
})
