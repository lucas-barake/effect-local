import { execFileSync } from "node:child_process"
import * as fs from "node:fs"
import * as http from "node:http"
import * as path from "node:path"

const [repoRoot, registryRoot] = process.argv.slice(2)
if (repoRoot === undefined || registryRoot === undefined) {
  throw new Error("Expected repository and registry roots")
}

const packages = new Map()
const pnpmModules = path.join(repoRoot, "node_modules/.pnpm")

for (const entry of fs.readdirSync(pnpmModules)) {
  const modules = path.join(pnpmModules, entry, "node_modules")
  if (!fs.existsSync(modules)) continue
  for (const name of fs.readdirSync(modules)) {
    const candidate = path.join(modules, name)
    const candidates = name.startsWith("@")
      ? fs.readdirSync(candidate).map((child) => path.join(candidate, child))
      : [candidate]
    for (const packageDirectory of candidates) {
      const manifestPath = path.join(packageDirectory, "package.json")
      if (!fs.existsSync(manifestPath) || fs.lstatSync(packageDirectory).isSymbolicLink()) continue
      const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"))
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
  if (existing !== undefined) return existing
  const result = JSON.parse(execFileSync(
    "npm",
    ["pack", "--json", "--ignore-scripts", "--pack-destination", tarballs, packageDirectory],
    { encoding: "utf8", maxBuffer: 10 * 1024 * 1024 }
  ))
  const filename = result[0]?.filename
  if (typeof filename !== "string") throw new Error(`npm pack did not return a filename for ${key}`)
  const tarball = path.join(tarballs, filename)
  packed.set(key, tarball)
  return tarball
}

const server = http.createServer((request, response) => {
  try {
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
      response.writeHead(200, { "content-type": "application/octet-stream" })
      fs.createReadStream(pack(name, found[0], found[1].packageDirectory)).pipe(response)
      return
    }

    const name = decodeURIComponent(requestUrl.pathname.slice(1))
    const versions = packages.get(name)
    if (versions === undefined) {
      response.writeHead(404, { "content-type": "application/json" }).end(JSON.stringify({ error: "not_found" }))
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
    response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({
      name,
      "dist-tags": { latest: entries.at(-1)?.[0] },
      versions: metadata
    }))
  } catch (error) {
    response.writeHead(500, { "content-type": "application/json" }).end(JSON.stringify({
      error: error instanceof Error ? error.message : String(error)
    }))
  }
})

let registryUrl
server.listen(0, "127.0.0.1", () => {
  const address = server.address()
  if (address === null || typeof address === "string") throw new Error("Registry did not bind a TCP port")
  registryUrl = `http://127.0.0.1:${address.port}`
  process.stdout.write(`${registryUrl}\n`)
})

const close = () => server.close(() => process.exit(0))
process.on("SIGTERM", close)
process.on("SIGINT", close)
