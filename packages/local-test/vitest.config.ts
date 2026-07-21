import { defineProject } from "vitest/config"

export default defineProject({ test: { name: "local-test", include: ["test/**/*.test.ts"] } })
