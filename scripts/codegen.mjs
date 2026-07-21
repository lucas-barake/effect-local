import { readdir, writeFile } from "node:fs/promises"
import { join } from "node:path"

await Promise.all(["local", "local-sql", "local-browser", "local-test"].map(async (directory) => {
  const source = join("packages", directory, "src")
  const files = (await readdir(source))
    .filter((file) => file.endsWith(".ts") && file !== "index.ts" && !file.startsWith("internal"))
    .toSorted()
  const output = files.map((file) => {
    const name = file.slice(0, -3)
    return `export * as ${name} from "./${name}.js"`
  }).join("\n")
  await writeFile(join(source, "index.ts"), output.length === 0 ? "export {}\n" : `${output}\n`)
}))
