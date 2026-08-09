import { join } from "node:path"
import { defineProject } from "vitest/config"

// expo-sqlite, expo-crypto, and react-native are native modules that cannot load under
// Node at all (expo-sqlite's own Node entry is an explicit dummy stub). They are the true
// external boundaries of this package, so tests alias them to faithful Node-backed fakes
// while the entire production graph above them (driver, SqlClient, SqlReplica, atoms)
// runs real.
export default defineProject({
  resolve: {
    alias: {
      "expo-sqlite": join(import.meta.dirname, "test/helpers/ExpoSqliteNode.ts"),
      "expo-crypto": join(import.meta.dirname, "test/helpers/ExpoCryptoNode.ts"),
      "react-native": join(import.meta.dirname, "test/helpers/ReactNative.ts")
    }
  },
  test: { name: "local-react-native", include: ["test/**/*.test.ts"] }
})
