import { defineProject } from "vitest/config"

export default defineProject({ test: { name: "local-browser", include: ["test/**/*.test.ts"] } })
