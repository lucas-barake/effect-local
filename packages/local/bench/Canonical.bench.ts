import { test } from "vitest"
import * as Canonical from "../src/Canonical.js"

const bytes = new Uint8Array(1024 * 1024)
for (let index = 0; index < bytes.length; index++) bytes[index] = index % 251
const value = { attachment: bytes }

test("canonical stringify 1 MiB Uint8Array", ({ bench }) =>
  bench("canonical stringify 1 MiB Uint8Array", () => {
    Canonical.stringify(value)
  }).run({ iterations: 50, time: 0, warmupIterations: 10, warmupTime: 0 }))
