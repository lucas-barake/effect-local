import { spawn } from "node:child_process"
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, symlinkSync } from "node:fs"
import { dirname, join, relative, resolve, sep } from "node:path"

const run = (command, args, options = {}) =>
  new Promise((resolveRun, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: process.env,
      stdio: "pipe"
    })
    const stdout = []
    const stderr = []
    child.stdout.on("data", (chunk) => stdout.push(chunk))
    child.stderr.on("data", (chunk) => stderr.push(chunk))
    child.on("error", reject)
    child.on("close", (code, signal) => {
      resolveRun({
        code: code ?? 1,
        signal,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8")
      })
    })
  })

const checked = async (command, args, options = {}) => {
  const result = await run(command, args, options)
  if (result.code !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed\n${result.stderr || result.stdout}`)
  }
  return result.stdout
}

const linkDependencies = (repository, snapshot) => {
  symlinkSync(join(repository, "node_modules"), join(snapshot, "node_modules"), "dir")
  const packages = join(repository, "packages")
  for (const name of readdirSync(packages)) {
    const source = join(packages, name, "node_modules")
    const target = join(snapshot, "packages", name, "node_modules")
    if (!existsSync(source) || !existsSync(dirname(target))) continue
    symlinkSync(source, target, "dir")
  }
}

const createSnapshots = async (repository) => {
  const temporary = mkdtempSync(join(repository, ".precommit-"))
  const baseline = join(temporary, "baseline")
  const staged = join(temporary, "staged")
  const archive = join(temporary, "baseline.tar")
  mkdirSync(baseline)
  mkdirSync(staged)
  await checked("git", ["archive", "--format=tar", `--output=${archive}`, "HEAD"], { cwd: repository })
  await checked("tar", ["-xf", archive, "-C", baseline])
  await checked("git", ["checkout-index", "--all", "--force", `--prefix=${staged}${sep}`], { cwd: repository })
  linkDependencies(repository, baseline)
  linkDependencies(repository, staged)
  return { baseline, staged, temporary }
}

const parseOxlint = (result, snapshot) => {
  let output
  try {
    output = JSON.parse(result.stdout)
  } catch {
    throw new Error(`Oxlint did not return JSON\n${result.stderr || result.stdout}`)
  }
  return output.diagnostics.map((diagnostic) => {
    const label = diagnostic.labels[0]
    const filename = relative(snapshot, resolve(snapshot, diagnostic.filename)).split(sep).join("/")
    return {
      key: `${filename}\0${diagnostic.code}\0${diagnostic.severity}\0${diagnostic.message}`,
      display: `${filename}:${label?.span.line ?? 1}:${
        label?.span.column ?? 1
      } ${diagnostic.code} ${diagnostic.message}`
    }
  })
}

const typeDiagnostic = /^(.*)\((\d+),(\d+)\): (error TS\d+: .*)$/

const parseTypecheck = (result, snapshot) => {
  const diagnostics = []
  for (const line of `${result.stdout}\n${result.stderr}`.split(/\r?\n/u)) {
    const match = typeDiagnostic.exec(line)
    if (match === null) continue
    const filename = relative(snapshot, resolve(snapshot, match[1])).split(sep).join("/")
    diagnostics.push({
      key: `${filename}\0${match[4]}`,
      display: `${filename}(${match[2]},${match[3]}): ${match[4]}`
    })
  }
  if (result.code !== 0 && diagnostics.length === 0) {
    throw new Error(`Typecheck failed without a TypeScript diagnostic\n${result.stderr || result.stdout}`)
  }
  return diagnostics
}

const parseFormatting = (result, snapshot) => {
  if (result.code !== 0 && result.code !== 20) {
    throw new Error(`dprint failed\n${result.stderr || result.stdout}`)
  }
  return result.stdout.split(/\r?\n/u).filter((line) => line.length > 0).map((filename) => {
    const normalized = relative(snapshot, resolve(snapshot, filename)).split(sep).join("/")
    return { key: normalized, display: `${normalized} is not formatted` }
  })
}

const introduced = (baseline, staged) => {
  const available = new Map()
  for (const diagnostic of baseline) available.set(diagnostic.key, (available.get(diagnostic.key) ?? 0) + 1)
  const result = []
  for (const diagnostic of staged) {
    const count = available.get(diagnostic.key) ?? 0
    if (count === 0) result.push(diagnostic.display)
    else available.set(diagnostic.key, count - 1)
  }
  return result
}

const main = async () => {
  const repository = (await checked("git", ["rev-parse", "--show-toplevel"])).trim()
  const { baseline, staged, temporary } = await createSnapshots(repository)
  try {
    const executable = (name) => join(repository, "node_modules", ".bin", name)
    await checked(process.execPath, ["scripts/test-oxlint-rules.mjs"], { cwd: staged })
    const commands = [
      [executable("oxlint"), ["--format=json", "--threads=1", "."]],
      [executable("tsgo"), ["-b", "tsconfig.json", "--pretty", "false"]],
      [executable("dprint"), ["check", "--list-different", "--allow-no-files"]]
    ]
    const [baselineResults, stagedResults] = await Promise.all([
      Promise.all(commands.map(([command, args]) => run(command, args, { cwd: baseline }))),
      Promise.all(commands.map(([command, args]) => run(command, args, { cwd: staged })))
    ])
    const failures = [
      ...new Set([
        ...introduced(
          parseOxlint(baselineResults[0], baseline),
          parseOxlint(stagedResults[0], staged)
        ),
        ...introduced(
          parseTypecheck(baselineResults[1], baseline),
          parseTypecheck(stagedResults[1], staged)
        ),
        ...introduced(
          parseFormatting(baselineResults[2], baseline),
          parseFormatting(stagedResults[2], staged)
        )
      ])
    ]
    if (failures.length > 0) {
      process.stderr.write(`Pre commit checks found ${failures.length} new diagnostic(s):\n${failures.join("\n")}\n`)
      process.exitCode = 1
    } else {
      process.stdout.write("Pre commit lint and typecheck passed with no new diagnostics.\n")
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true })
  }
}

await main()
