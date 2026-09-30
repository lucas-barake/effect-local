import { dmConversationId, UserId } from "@effect-local/example-chat-shared/domain"
import { execFile } from "node:child_process"
import { join } from "node:path"
import { promisify } from "node:util"
import { expect, incoming, openDirectMessage, outgoing, send, test, uniqueText } from "../e2e/fixtures.js"

const run = promisify(execFile)
const flows = join(import.meta.dirname, "flows")

const maestro = (flow: string, env: Readonly<Record<string, string>>) =>
  run(
    "maestro",
    ["test", ...Object.entries(env).flatMap(([key, value]) => ["-e", `${key}=${value}`]), join(flows, flow)],
    { maxBuffer: 16 * 1024 * 1024 }
  )

const clearAndroidAppData = async () => {
  if (process.env.MOBILE_PLATFORM === "ios") return
  await run("adb", ["shell", "pm", "clear", "dev.effectlocal.chat"])
}

test("a web user and an Expo user chat both ways through one server", async ({ chat }) => {
  const alice = await chat.signIn("alice")
  await clearAndroidAppData()
  await maestro("login.yaml", { USER: "bob" })

  await openDirectMessage(alice, "bob")
  const fromWeb = uniqueText("from web")
  await send(alice, fromWeb)

  const conversation = dmConversationId(UserId.make("alice"), UserId.make("bob"))
  await maestro("open-conversation.yaml", { CONVERSATION: conversation, TEXT: fromWeb })
  await expect(outgoing(alice, fromWeb).locator(".tick-read")).toBeVisible()

  const fromMobile = uniqueText("from mobile")
  await maestro("send.yaml", { TEXT: fromMobile })
  await expect(incoming(alice, fromMobile)).toBeVisible()
})
