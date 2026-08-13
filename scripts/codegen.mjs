import { readdir, writeFile } from "node:fs/promises"
import { join } from "node:path"

const writes = ["local", "local-sql", "local-browser", "local-rpc", "local-test"].map(async (directory) => {
  const source = join("packages", directory, "src")
  const files = (await readdir(source))
    .filter((file) => file.endsWith(".ts") && file !== "index.ts" && !file.startsWith("internal"))
    .toSorted()
  const output = files.map((file) => {
    const name = file.slice(0, -3)
    return `export * as ${name} from "./${name}.js"`
  }).join("\n")
  let contents = `${output}\n`
  if (output.length === 0) contents = "export {}\n"
  await writeFile(join(source, "index.ts"), contents)
})

await Promise.all(writes)
