import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- This fixture is a real Node-only package registry process and must preserve its child-process, archive, and HTTP boundaries.
import { execFileSync } from "node:child_process"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- The registry packages and tarballs are read and written through the host filesystem.
import * as fs from "node:fs"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- The fixture is the real local HTTP registry process used by the dependency installation tests.
import * as http from "node:http"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- Registry URLs and archive paths must use the host path implementation.
import * as path from "node:path"
import nodeProcess from "node:process"

// oxlint-disable-next-line effect/noNodeBuiltinImport -- The registry must read its roots from the real host process arguments.
const [repoRoot, registryRoot] = nodeProcess.argv.slice(2)
if (repoRoot === undefined || registryRoot === undefined) {
  // oxlint-disable-next-line effect/noNodeBuiltinImport -- Invalid registry startup diagnostics must use the real host stderr stream.
  nodeProcess.stderr.write("Expected repository and registry roots\n")
  // oxlint-disable-next-line effect/noNodeBuiltinImport -- Invalid registry startup must use the real host process exit behavior.
  nodeProcess.exit(1)
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

const tarballFilename = (name, version) => `${name.replace(/^@/, "").replaceAll("/", "-")}-${version}.tgz`

const pack = (name, version, packageDirectory) => {
  const key = `${name}@${version}`
  const existing = packed.get(key)
  if (existing !== undefined) return Effect.succeed(existing)
  const filename = tarballFilename(name, version)
  const tarball = path.join(tarballs, filename)
  return Effect.acquireUseRelease(
    Effect.try({
      try: () => fs.mkdtempSync(path.join(registryRoot, "pack-")),
      catch: (cause) => cause
    }),
    (staging) => Effect.try({
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
    }),
    (staging) => Effect.try({
      try: () => fs.rmSync(staging, { recursive: true, force: true }),
      catch: (cause) => cause
    })
  )
}

const jsonText = (value) => Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(value)
const errorMessage = (error) => {
  if (error instanceof Error) return error.message
  return String(error)
}
const errorValue = (error) => {
  if (error instanceof Error) return error
  return new Error(String(error)) // oxlint-disable-line effect/noNewError -- response.destroy requires an Error instance and the failure may be any value from the handler Effect.
}

const handleRequest = (request, response) => Effect.gen(function*() {
  const requestUrl = yield* Effect.try({
    try: () => new URL(request.url ?? "/", "http://127.0.0.1"),
    catch: (cause) => cause
  })
  const tarballSeparator = requestUrl.pathname.indexOf("/-/")
  if (tarballSeparator !== -1) {
    const name = yield* Effect.try({
      try: () => decodeURIComponent(requestUrl.pathname.slice(1, tarballSeparator)),
      catch: (cause) => cause
    })
    const filename = path.basename(requestUrl.pathname)
    const found = Array.from(packages.get(name)?.entries() ?? [])
      .find(([version]) => tarballFilename(name, version) === filename)
    if (found === undefined) {
      yield* Effect.try({
        try: () => response.writeHead(404).end(),
        catch: (cause) => cause
      })
      return
    }
    const tarball = yield* pack(name, found[0], found[1].packageDirectory)
    yield* Effect.try({
      try: () => response.writeHead(200, { "content-type": "application/octet-stream" }),
      catch: (cause) => cause
    })
    yield* Effect.try({
      try: () => fs.createReadStream(tarball).pipe(response),
      catch: (cause) => cause
    })
    return
  }

  const name = yield* Effect.try({
    try: () => decodeURIComponent(requestUrl.pathname.slice(1)),
    catch: (cause) => cause
  })
  const versions = packages.get(name)
  if (versions === undefined) {
    const body = yield* Effect.try({
      try: () => jsonText({ error: "not_found" }),
      catch: (cause) => cause
    })
    yield* Effect.try({
      try: () => response.writeHead(404, { "content-type": "application/json" }).end(body),
      catch: (cause) => cause
    })
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
  const body = yield* Effect.try({
    try: () => jsonText({
      name,
      "dist-tags": { latest: entries.at(-1)?.[0] },
      versions: metadata
    }),
    catch: (cause) => cause
  })
  yield* Effect.try({
    try: () => response.writeHead(200, { "content-type": "application/json" }).end(body),
    catch: (cause) => cause
  })
})

const server = http.createServer((request, response) => {
  Effect.runPromise(handleRequest(request, response)).catch((error) => {
    const body = jsonText({ error: errorMessage(error) })
    if (response.headersSent) response.destroy(errorValue(error))
    else response.writeHead(500, { "content-type": "application/json" }).end(body)
  })
})

let registryUrl
server.listen(0, "127.0.0.1", () => {
  const address = server.address()
  if (address === null || typeof address === "string") {
    // oxlint-disable-next-line effect/noNodeBuiltinImport -- Registry startup diagnostics must use the real host stderr stream.
    nodeProcess.stderr.write("Registry did not bind a TCP port\n")
    // oxlint-disable-next-line effect/noNodeBuiltinImport -- Registry startup must use the real host process exit behavior.
    nodeProcess.exit(1)
  }
  registryUrl = `http://127.0.0.1:${address.port}`
  // oxlint-disable-next-line effect/noNodeBuiltinImport -- The parent test process discovers the real registry through the host stdout stream.
  nodeProcess.stdout.write(`${registryUrl}\n`)
})

const close = () => server.close(() => {
  // oxlint-disable-next-line effect/noNodeBuiltinImport -- Signal shutdown must use the real host process exit behavior.
  nodeProcess.exit(0)
})
nodeProcess.on("SIGTERM", close)
nodeProcess.on("SIGINT", close)
