import { expect, test as base } from "@playwright/test"
import { mkdtemp, rm } from "node:fs/promises"
import { createServer as createHttpServer } from "node:http"
import type { AddressInfo } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"
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

const withEnvironment = async <A,>(
  env: Readonly<Record<string, string>>,
  evaluate: () => Promise<A>
): Promise<A> => {
  const previous = new Map<string, string | undefined>()
  for (const [key, value] of Object.entries(env)) {
    previous.set(key, process.env[key])
    process.env[key] = value
  }
  try {
    return await evaluate()
  } finally {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
  }
}

const serverUrl = (server: { address(): string | AddressInfo | null }) => {
  const address = server.address()
  if (address === null || address === undefined || typeof address === "string") {
    throw new Error("Vite did not bind a TCP port")
  }
  return `http://127.0.0.1:${address.port}`
}

export const makeViteTest = (options: Options) => {
  const test = base.extend<{}, { readonly appServer: { readonly url: string } }>({
    // Playwright parses this parameter and requires an object destructuring pattern.
    // oxlint-disable-next-line no-empty-pattern
    appServer: [async ({}, use) => {
      const dependency = options.dependency === undefined ? undefined : await options.dependency()
      const env = { ...options.env, ...dependency?.env }
      let outDir: string | undefined
      let server: { readonly url: string; readonly close: () => Promise<void> } | undefined
      const successful = Symbol()
      let fixtureFailure: unknown = successful
      try {
        if (options.mode === "development") {
          const httpServer = createHttpServer()
          const vite = await withEnvironment(env, () =>
            createServer({
              configFile: options.configFile,
              logLevel: "error",
              server: { hmr: { server: httpServer }, middlewareMode: true }
            }))
          httpServer.on("request", vite.middlewares)
          try {
            await new Promise<void>((resolve, reject) => {
              const onError = (error: Error) => {
                httpServer.off("listening", onListening)
                reject(error)
              }
              const onListening = () => {
                httpServer.off("error", onError)
                resolve()
              }
              httpServer.once("error", onError)
              httpServer.once("listening", onListening)
              httpServer.listen({ host: "127.0.0.1", port: 0 })
            })
          } catch (error) {
            await vite.close()
            throw error
          }
          server = {
            url: serverUrl(httpServer),
            close: async () => {
              const results = await Promise.allSettled([
                new Promise<void>((resolve, reject) => {
                  httpServer.close((error) => error === undefined ? resolve() : reject(error))
                }),
                vite.close()
              ])
              const failures = results.flatMap((result) => result.status === "rejected" ? [result.reason] : [])
              if (failures.length > 0) throw new AggregateError(failures, "Vite development teardown failed")
            }
          }
        } else {
          outDir = await mkdtemp(join(tmpdir(), "effect-local-playwright-"))
          await withEnvironment(env, () =>
            build({
              configFile: options.configFile,
              logLevel: "error",
              build: { outDir }
            }).then(() => undefined))
          const vite = await withEnvironment(env, () =>
            preview({
              configFile: options.configFile,
              logLevel: "error",
              build: { outDir },
              preview: { host: "127.0.0.1", port: 0, strictPort: true }
            }))
          server = { url: serverUrl(vite.httpServer), close: () => vite.close() }
        }
        await use({ url: server.url })
      } catch (error) {
        fixtureFailure = error
      }
      const results = await Promise.allSettled([
        server?.close(),
        dependency?.close(),
        outDir === undefined ? undefined : rm(outDir, { recursive: true, force: true })
      ])
      const failures = results.flatMap((result) => result.status === "rejected" ? [result.reason] : [])
      if (fixtureFailure !== successful) failures.unshift(fixtureFailure)
      if (failures.length === 1) throw failures[0]
      if (failures.length > 1) throw new AggregateError(failures, "Playwright server fixture failed")
    }, { scope: "worker" }],
    baseURL: async ({ appServer }, use) => {
      await use(appServer.url)
    }
  })
  return { expect, test }
}
