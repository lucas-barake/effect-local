import { defineProject } from "vitest/config"

export default defineProject({
  test: {
    name: "local-rpc",
    include: ["test/**/*.test.ts"],
    globalSetup: ["test/fixtures/postgresGlobalSetup.ts"]
  }
})
