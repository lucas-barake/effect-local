import { fileURLToPath } from "node:url"
import { defineConfig } from "vitest/config"

const fixture = (path: string) => fileURLToPath(new URL(path, import.meta.url))

export default defineConfig({
  resolve: {
    alias: {
      "expo": fixture("../../packages/local-expo/test/fixtures/expo.ts"),
      "expo-modules-core": fixture("../../packages/local-expo/test/fixtures/expoModulesCore.ts"),
      "expo-asset": fixture("../../packages/local-expo/test/fixtures/expoAsset.ts"),
      "expo-secure-store": fixture("./test/fixtures/expoSecureStore.ts"),
      "react-native": fixture("../../packages/local-expo/test/fixtures/reactNative.ts")
    }
  },
  test: {
    include: ["test/**/*.test.ts"],
    server: { deps: { inline: ["expo-sqlite", "expo-crypto"] } }
  }
})
