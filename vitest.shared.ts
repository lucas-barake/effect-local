import { defineProject } from "vitest/config"

export const definePackageProject = (name: string) =>
  defineProject({
    test: {
      name,
      include: ["test/**/*.test.ts"]
    }
  })
