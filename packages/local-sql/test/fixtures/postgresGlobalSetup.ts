import { PostgreSqlContainer } from "@testcontainers/postgresql"
import type { TestProject } from "vitest/node"

declare module "vitest" {
  export interface ProvidedContext {
    readonly postgresUrl: string
    readonly postgresContainerId: string
  }
}

export default function setup(project: TestProject) {
  return new PostgreSqlContainer("postgres:16-alpine")
    .withCommand([
      "postgres",
      "-c",
      "max_connections=1000",
      "-c",
      "fsync=off",
      "-c",
      "synchronous_commit=off",
      "-c",
      "full_page_writes=off",
      "-c",
      "deadlock_timeout=20ms"
    ])
    .start()
    .then((container) => {
      project.provide("postgresUrl", container.getConnectionUri())
      project.provide("postgresContainerId", container.getId())
      return () => container.stop().then(() => undefined)
    })
}
