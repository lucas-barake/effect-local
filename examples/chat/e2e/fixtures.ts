import { type Browser, type BrowserContext, expect, type Page, test as base } from "@playwright/test"
import { writeFileSync } from "node:fs"

export type UserId = "alice" | "bob" | "carol" | "dave"

const names: Record<UserId, string> = { alice: "Alice", bob: "Bob", carol: "Carol", dave: "Dave" }

export interface Chat {
  readonly signIn: (user: UserId) => Promise<Page>
  readonly openTab: (page: Page, label: string, path?: string) => Promise<Page>
  readonly openLoginPage: () => Promise<Page>
}

const capture = (page: Page, label: string, lines: Array<string>): void => {
  page.on("console", (message) => lines.push(`[${label}:${message.type()}] ${message.text()}`))
  page.on("pageerror", (error) => lines.push(`[${label}:pageerror] ${error.message}`))
}

const recordFalseStates = () => {
  const seen = new Set<string>()
  const falseState = (line: string) =>
    line === "offline" ||
    line.startsWith("Offline") ||
    line.startsWith("Sync failed") ||
    line.startsWith("No conversations yet") ||
    line.startsWith("No messages yet. Say hello!")
  const scan = () => {
    const text = document.body?.innerText ?? ""
    for (const line of text.split("\n")) {
      const trimmed = line.trim()
      if (falseState(trimmed)) seen.add(trimmed)
    }
  }
  new MutationObserver(scan).observe(document, { subtree: true, childList: true, characterData: true })
  Reflect.set(globalThis, "__chatFalseStates", seen)
}

export const falseStates = (page: Page): Promise<Array<string>> =>
  page.evaluate(() => {
    const seen: unknown = Reflect.get(globalThis, "__chatFalseStates")
    if (!(seen instanceof Set)) return ["recorder missing"]
    return [...seen].map(String)
  })

const newContext = async (browser: Browser, contexts: Array<BrowserContext>): Promise<BrowserContext> => {
  const context = await browser.newContext()
  contexts.push(context)
  await context.addInitScript(recordFalseStates)
  return context
}

const closeAll = async (contexts: ReadonlyArray<BrowserContext>): Promise<void> => {
  const [head, ...rest] = contexts
  if (head === undefined) return
  await head.close()
  await closeAll(rest)
}

const openTab = async (page: Page, label: string, path: string, lines: Array<string>): Promise<Page> => {
  const tab = await page.context().newPage()
  capture(tab, label, lines)
  await tab.goto(path)
  return tab
}

const openLoginPage = async (
  browser: Browser,
  contexts: Array<BrowserContext>,
  lines: Array<string>
): Promise<Page> => {
  const context = await newContext(browser, contexts)
  const page = await context.newPage()
  capture(page, "login", lines)
  await page.goto("/")
  return page
}

const signIn = async (
  browser: Browser,
  contexts: Array<BrowserContext>,
  user: UserId,
  lines: Array<string>
): Promise<Page> => {
  const context = await newContext(browser, contexts)
  const page = await context.newPage()
  capture(page, user, lines)
  await page.goto("/")
  await page.locator(".login-user", { hasText: names[user] }).click()
  await page.locator(".login-password").fill(`${user}123`)
  await page.locator(".login-submit").click()
  await expect(page.locator(".sidebar-me")).toHaveText(names[user])
  return page
}

export const test = base.extend<{ readonly chat: Chat }>({
  chat: async ({ browser }, use, testInfo) => {
    const lines: Array<string> = []
    const contexts: Array<BrowserContext> = []
    await use({
      signIn: (user) => signIn(browser, contexts, user, lines),
      openTab: (page, label, path = "/") => openTab(page, label, path, lines),
      openLoginPage: () => openLoginPage(browser, contexts, lines)
    })
    await closeAll(contexts)
    if (testInfo.status === testInfo.expectedStatus) return
    const path = testInfo.outputPath("console.txt")
    writeFileSync(path, lines.join("\n"))
    await testInfo.attach("console", { path, contentType: "text/plain" })
  }
})

export { expect }

export const openDirectMessage = async (page: Page, peer: UserId): Promise<void> => {
  const existing = page.locator(".conversation", { hasText: names[peer] })
  const start = page.locator(".sidebar-new-row", { hasText: names[peer] })
  await existing.or(start).first().click()
  await expect(page.locator(".chat-title")).toHaveText(names[peer])
}

export const expectOnline = async (page: Page, peer: UserId): Promise<void> => {
  const row = page.locator(".conversation, .sidebar-new-row", { hasText: names[peer] })
  await expect(row.getByRole("img", { name: "online" })).toBeVisible()
}

export const send = async (page: Page, text: string): Promise<void> => {
  await page.locator(".chat-input").fill(text)
  await page.locator(".chat-send").click()
}

export const sendAll = async (page: Page, texts: ReadonlyArray<string>): Promise<void> => {
  const [head, ...rest] = texts
  if (head === undefined) return
  await send(page, head)
  await sendAll(page, rest)
}

export const outgoing = (page: Page, text: string) => page.locator(".bubble-row-out", { hasText: text })

export const incoming = (page: Page, text: string) => page.locator(".bubble-row-in", { hasText: text })

export const uniqueText = (label: string): string => `${label} ${crypto.randomUUID().slice(0, 8)}`
