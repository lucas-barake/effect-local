import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- This fixture is a real Node-only package registry process and must preserve its child-process, archive, and HTTP boundaries.
import { execFileSync } from "node:child_process"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- The registry packages and tarballs are read and written through the host filesystem.
import * as fs from "node:fs"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- The fixture is the real local HTTP registry process used by the dependency installation tests.
import * as http from "node:http"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- Registry URLs and archive paths must use the host path implementation.
import * as path from "node:path"
const nodeProcess = globalThis.process

const [repoRoot, registryRoot] = nodeProcess.argv.slice(2)
if (repoRoot === undefined || registryRoot === undefined) {
  Effect.runSync(Effect.die(new Error("Expected repository and registry roots")))
}

const packages = new Map()
const pnpmModules = path.join(repoRoot, "node_modules/.pnpm")

for (const entry of fs.readdirSync(pnpmModules)) {
  const modules = path.join(pnpmModules, entry, "node_modules")
  if (!fs.existsSync(modules)) continue
  for (const name of fs.readdirSync(modules)) {
    const candidate = path.join(modules, name)
    let candidates
    if (name.startsWith("@")) candidates = fs.readdirSync(candidate).map((child) => path.join(candidate, child))
    else candidates = [candidate]
    for (const packageDirectory of candidates) {
      const manifestPath = path.join(packageDirectory, "package.json")
      if (!fs.existsSync(manifestPath) || fs.lstatSync(packageDirectory).isSymbolicLink()) continue
      const manifest = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(
        fs.readFileSync(manifestPath, "utf8")
      )
      if (typeof manifest.name !== "string" || typeof manifest.version !== "string") continue
      const versions = packages.get(manifest.name) ?? new Map()
      versions.set(manifest.version, { manifest, packageDirectory })
      packages.set(manifest.name, versions)
    }
  }
}

const tarballs = path.join(registryRoot, "tarballs")
fs.mkdirSync(tarballs, { recursive: true })
const packed = new Map()
const runSyncThrowing = (thunk) =>
  Effect.runSync(Effect.try({ try: thunk, catch: (cause) => cause }).pipe(Effect.orDie))

const tarballFilename = (name, version) => `${name.replace(/^@/, "").replaceAll("/", "-")}-${version}.tgz`

const pack = (name, version, packageDirectory) => {
  const key = `${name}@${version}`
  const existing = packed.get(key)
  if (existing !== undefined) return existing
  const filename = tarballFilename(name, version)
  const tarball = path.join(tarballs, filename)
  const staging = runSyncThrowing(() => fs.mkdtempSync(path.join(registryRoot, "pack-")))
  const result = Effect.runSyncExit(Effect.try({
    try: () => {
      fs.cpSync(packageDirectory, path.join(staging, "package"), {
        recursive: true,
        filter: (source) => path.basename(source) !== "node_modules"
      })
      execFileSync("tar", ["-czf", tarball, "-C", staging, "package"])
      packed.set(key, tarball)
      return tarball
    },
    catch: (cause) => cause
  }).pipe(Effect.orDie))
  runSyncThrowing(() => fs.rmSync(staging, { recursive: true, force: true }))
  if (Exit.isFailure(result)) Effect.runSync(Effect.die(Cause.squash(result.cause)))
  return result.value
}

const jsonText = (value) => Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(value)
const errorMessage = (error) => {
  if (error instanceof Error) return error.message
  return String(error)
}
const errorValue = (error) => {
  if (error instanceof Error) return error
  return globalThis.Error(String(error))
}

const server = http.createServer((request, response) => {
  const result = Effect.runSyncExit(Effect.try({
    try: () => {
      const requestUrl = new URL(request.url ?? "/", "http://127.0.0.1")
      const tarballSeparator = requestUrl.pathname.indexOf("/-/")
      if (tarballSeparator !== -1) {
        const name = decodeURIComponent(requestUrl.pathname.slice(1, tarballSeparator))
        const filename = path.basename(requestUrl.pathname)
        const found = Array.from(packages.get(name)?.entries() ?? [])
          .find(([version]) => tarballFilename(name, version) === filename)
        if (found === undefined) {
          response.writeHead(404).end()
          return
        }
        const tarball = pack(name, found[0], found[1].packageDirectory)
        response.writeHead(200, { "content-type": "application/octet-stream" })
        fs.createReadStream(tarball).pipe(response)
        return
      }

      const name = decodeURIComponent(requestUrl.pathname.slice(1))
      const versions = packages.get(name)
      if (versions === undefined) {
        response.writeHead(404, { "content-type": "application/json" }).end(
          jsonText({ error: "not_found" })
        )
        return
      }
      const entries = Array.from(versions.entries())
      const metadata = Object.fromEntries(entries.map(([version, value]) => {
        return [version, {
          ...value.manifest,
          dist: {
            tarball: `${registryUrl}/${encodeURIComponent(name)}/-/${tarballFilename(name, version)}`
          }
        }]
      }))
      response.writeHead(200, { "content-type": "application/json" }).end(jsonText({
        name,
        "dist-tags": { latest: entries.at(-1)?.[0] },
        versions: metadata
      }))
    },
    catch: (cause) => cause
  }).pipe(Effect.orDie))
  if (Exit.isFailure(result)) {
    const error = Cause.squash(result.cause)
    const body = jsonText({ error: errorMessage(error) })
    if (response.headersSent) response.destroy(errorValue(error))
    else response.writeHead(500, { "content-type": "application/json" }).end(body)
  }
})

let registryUrl
server.listen(0, "127.0.0.1", () => {
  const address = server.address()
  if (address === null || typeof address === "string") {
    Effect.runSync(Effect.die(new Error("Registry did not bind a TCP port")))
  }
  registryUrl = `http://127.0.0.1:${address.port}`
  nodeProcess.stdout.write(`${registryUrl}\n`)
})

const close = () => server.close(() => nodeProcess.exit(0))
nodeProcess.on("SIGTERM", close)
nodeProcess.on("SIGINT", close)
