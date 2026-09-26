import { type Browser, expect, type Page, test as base } from "@playwright/test"
import { writeFileSync } from "node:fs"

export type UserId = "alice" | "bob" | "carol" | "dave"

export const names: Record<UserId, string> = { alice: "Alice", bob: "Bob", carol: "Carol", dave: "Dave" }

export interface Chat {
  readonly signIn: (user: UserId) => Promise<Page>
  readonly openTab: (page: Page, label: string) => Promise<Page>
}

const capture = (page: Page, label: string, lines: Array<string>): void => {
  page.on("console", (message) => lines.push(`[${label}:${message.type()}] ${message.text()}`))
  page.on("pageerror", (error) => lines.push(`[${label}:pageerror] ${error.message}`))
}

const openTab = async (page: Page, label: string, lines: Array<string>): Promise<Page> => {
  const tab = await page.context().newPage()
  capture(tab, label, lines)
  await tab.goto("/")
  return tab
}

const signIn = async (browser: Browser, user: UserId, lines: Array<string>): Promise<Page> => {
  const context = await browser.newContext()
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
    await use({
      signIn: (user) => signIn(browser, user, lines),
      openTab: (page, label) => openTab(page, label, lines)
    })
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
  await expect(existing.or(start).first()).toBeVisible()
  if (await existing.count() > 0) await existing.first().click()
  else await start.click()
  await expect(page.locator(".chat-title")).toHaveText(names[peer])
}

export const send = async (page: Page, text: string): Promise<void> => {
  await page.locator(".chat-input").fill(text)
  await page.locator(".chat-send").click()
}

export const outgoing = (page: Page, text: string) => page.locator(".bubble-row-out", { hasText: text })

export const incoming = (page: Page, text: string) => page.locator(".bubble-row-in", { hasText: text })

export const uniqueText = (label: string): string => `${label} ${crypto.randomUUID().slice(0, 8)}`
