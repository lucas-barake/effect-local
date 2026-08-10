import { NodeFileSystem } from "@effect/platform-node"
import { expect, test as base } from "@playwright/test"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as FileSystem from "effect/FileSystem"
import { build, createServer, preview } from "vite"

interface FixtureDependency {
  readonly env?: Readonly<Record<string, string>>
  readonly close: () => Promise<void>
}

interface Options {
  readonly configFile: string
  readonly mode: "development" | "production"
  readonly env?: Readonly<Record<string, string>>
  readonly dependency?: () => Promise<FixtureDependency>
}

const withEnvironment = <A,>(
  env: Readonly<Record<string, string>>,
  evaluate: () => Effect.Effect<A, unknown>
): Effect.Effect<A, unknown> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const previous = new Map<string, string | undefined>()
      for (const [key, value] of Object.entries(env)) {
        previous.set(key, globalThis.process.env[key])
        globalThis.process.env[key] = value
      }
      return previous
    }),
    evaluate,
    (previous) =>
      Effect.sync(() => {
        for (const [key, value] of previous) {
          if (value === undefined) delete globalThis.process.env[key]
          else globalThis.process.env[key] = value
        }
      })
  )

const serverUrl = (server: { address(): string | { readonly port: number } | null } | null) => {
  if (server === null) return Effect.die(new Error("Vite did not create an HTTP server"))
  const address = server.address()
  if (address === null || typeof address === "string") {
    return Effect.die(new Error("Vite did not bind a TCP port"))
  }
  return Effect.succeed(`http://127.0.0.1:${address.port}`)
}

export const makeViteTest = (options: Options) => {
  const test = base.extend<{}, { readonly appServer: { readonly url: string } }>({
    appServer: [({ browserName: _browserName }, use) =>
      Effect.runPromise(
        Effect.gen(function*() {
          const fileSystem = yield* FileSystem.FileSystem
          let dependency: FixtureDependency | undefined
          if (options.dependency !== undefined) {
            dependency = yield* Effect.tryPromise({
              try: options.dependency,
              catch: (error) => error
            })
          }
          const env = { ...options.env, ...dependency?.env }
          let server: { readonly url: string; readonly close: () => Promise<void> } | undefined
          let outDir: string | undefined
          const successful = Symbol()
          let fixtureFailure: unknown = successful
          const fixtureExit = yield* Effect.exit(Effect.gen(function*() {
            if (options.mode === "development") {
              const vite = yield* withEnvironment(env, () =>
                Effect.tryPromise({
                  try: () =>
                    createServer({
                      configFile: options.configFile,
                      logLevel: "error",
                      server: { host: "127.0.0.1", port: 0 }
                    }),
                  catch: (error) => error
                }))
              const close = () => vite.close()
              server = { url: "", close }
              yield* withEnvironment(env, () =>
                Effect.tryPromise({
                  try: () => vite.listen(),
                  catch: (error) => error
                }))
              const url = yield* serverUrl(vite.httpServer)
              server = { url, close }
            } else {
              outDir = yield* fileSystem.makeTempDirectory({ prefix: "effect-local-playwright-" })
              yield* withEnvironment(env, () =>
                Effect.tryPromise({
                  try: () =>
                    build({
                      configFile: options.configFile,
                      logLevel: "error",
                      build: { outDir }
                    }).then(() => undefined),
                  catch: (error) => error
                }))
              const vite = yield* withEnvironment(env, () =>
                Effect.tryPromise({
                  try: () =>
                    preview({
                      configFile: options.configFile,
                      logLevel: "error",
                      build: { outDir },
                      preview: { host: "127.0.0.1", port: 0, strictPort: true }
                    }),
                  catch: (error) => error
                }))
              const close = () => vite.close()
              server = { url: "", close }
              const url = yield* serverUrl(vite.httpServer)
              server = { url, close }
            }
            const appServer = server
            if (appServer === undefined) yield* Effect.die(new Error("Vite server was not initialized"))
            yield* Effect.tryPromise({
              try: () => use({ url: appServer.url }),
              catch: (error) => error
            })
          }))
          if (Exit.isFailure(fixtureExit)) {
            fixtureFailure = Cause.squash(fixtureExit.cause)
          }
          const cleanup: Array<Effect.Effect<unknown, unknown>> = []
          if (server !== undefined) cleanup.push(Effect.tryPromise({ try: server.close, catch: (error) => error }))
          if (dependency !== undefined) {
            cleanup.push(Effect.tryPromise({ try: dependency.close, catch: (error) => error }))
          }
          if (outDir !== undefined) cleanup.push(fileSystem.remove(outDir, { recursive: true, force: true }))
          const [cleanupFailures] = yield* Effect.partition(cleanup, (effect) => effect, { concurrency: "unbounded" })
          const failures = [...cleanupFailures]
          if (fixtureFailure !== successful) failures.unshift(fixtureFailure)
          if (failures.length === 1) yield* Effect.die(failures[0])
          if (failures.length > 1) yield* Effect.die(new AggregateError(failures, "Playwright server fixture failed"))
        }).pipe(Effect.provide(NodeFileSystem.layer))
      ), { scope: "worker" }],
    baseURL: ({ appServer }, use) => use(appServer.url)
  })
  return { expect, test }
}
