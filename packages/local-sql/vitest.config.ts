import { defineProject } from "vitest/config"

export default defineProject({ test: { name: "local-sql", include: ["test/**/*.test.ts"] } })
