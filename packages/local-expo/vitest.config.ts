import { fileURLToPath } from "node:url"
import { defineProject } from "vitest/config"

const fixture = (name: string) => fileURLToPath(new URL(`./test/fixtures/${name}.ts`, import.meta.url))

export default defineProject({
  resolve: {
    alias: {
      "expo": fixture("expo"),
      "expo-modules-core": fixture("expoModulesCore"),
      "expo-asset": fixture("expoAsset"),
      "react-native": fixture("reactNative")
    }
  },
  test: {
    name: "local-expo",
    include: ["test/**/*.test.ts"],
    server: { deps: { inline: ["expo-sqlite", "expo-crypto"] } }
  }
})
