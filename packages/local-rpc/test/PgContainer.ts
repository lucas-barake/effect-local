import { PgClient } from "@effect/sql-pg"
import { PostgreSqlContainer } from "@testcontainers/postgresql"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"

/**
 * The PostgreSQL container the relay suites run against, and the failure of starting one.
 *
 * Shared because the image tag and the start/stop lifetime are one decision rather than one per
 * suite. Both entry points exist on purpose: a suite that only wants a client on the default
 * database takes `layerClient`, and a suite that opens more than one database on the same container
 * takes `layer` and derives its own clients from the running container's URI.
 */
export class ContainerError extends Data.TaggedError("ContainerError")<{
  readonly cause: unknown
}> {}

export class PgContainer extends Context.Service<PgContainer>()(
  "@lucas-barake/effect-local-rpc/test/PgContainer",
  {
    make: Effect.acquireRelease(
      Effect.tryPromise({
        try: () => new PostgreSqlContainer("postgres:alpine").start(),
        catch: (cause) => new ContainerError({ cause })
      }),
      (container) => Effect.promise(() => container.stop())
    )
  }
) {
  static readonly layer = Layer.effect(this)(this.make)

  static readonly layerClient = Layer.unwrap(
    Effect.gen(function*() {
      const container = yield* PgContainer
      return PgClient.layer({ url: Redacted.make(container.getConnectionUri()) })
    })
  ).pipe(Layer.provide(this.layer))
}
