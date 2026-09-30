import type { LoginResponse } from "@effect-local/example-chat-shared/auth"
import type { Connection } from "@effect-local/example-chat-shared/connection"
import { type ConversationId, groupConversationId, UserId } from "@effect-local/example-chat-shared/domain"
import { RegistryProvider, useAtomMount, useAtomSet, useAtomValue } from "@effect/atom-react"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import { StatusBar } from "expo-status-bar"
import { useState } from "react"
import { Pressable, SafeAreaView, StyleSheet, Text, View } from "react-native"
import { Chat } from "./Chat.js"
import { Conversations } from "./Conversations.js"
import { Login } from "./Login.js"
import { clientFor, logoutAtom, sessionAtom } from "./runtime.js"
import { colors } from "./theme.js"

const bannerText: Record<Connection, string | undefined> = {
  online: undefined,
  connecting: undefined,
  idle: undefined,
  superseded: undefined,
  needsAuthentication: "Session expired.",
  failed: "Sync failed. Local data remains available.",
  offline: "Offline. Messages send when the connection returns."
}

const Banner = ({ connection }: { readonly connection: Connection }) => {
  const logout = useAtomSet(logoutAtom)
  const text = bannerText[connection]
  if (text === undefined) return null
  return (
    <View testID={`banner-${connection}`} accessibilityRole="alert" style={styles.banner}>
      <Text style={styles.bannerText}>{text}</Text>
      {connection === "needsAuthentication" && (
        <Pressable accessibilityRole="button" onPress={() => logout(undefined)}>
          <Text style={styles.bannerAction}>Sign in again</Text>
        </Pressable>
      )}
    </View>
  )
}

const Home = ({ session }: { readonly session: LoginResponse }) => {
  const client = clientFor(session)
  useAtomMount(client.presenceAtom)
  useAtomMount(client.deliveryDaemon)
  useAtomMount(client.settlementDaemon)
  const connection = AsyncResult.getOrElse(useAtomValue(client.connectionAtom), (): Connection => "connecting")
  const startConversation = useAtomSet(client.startConversation)
  const [openId, setOpenId] = useState<ConversationId | null>(null)
  const me = session.userId

  const open = (conversationId: ConversationId) => {
    if (conversationId === groupConversationId) {
      startConversation({ kind: "group" })
    } else {
      const peer = conversationId.slice("dm:".length).split(":").find((memberId) => memberId !== me)
      if (peer !== undefined) startConversation({ kind: "dm", userId: UserId.make(peer) })
    }
    setOpenId(conversationId)
  }

  return (
    <View style={styles.fill}>
      <Banner connection={connection} />
      {openId === null && <Conversations client={client} session={session} onOpen={open} />}
      {openId !== null && (
        <Chat
          key={openId}
          client={client}
          me={me}
          conversationId={openId}
          onBack={() => setOpenId(null)}
        />
      )}
    </View>
  )
}

const Root = () => {
  const session = useAtomValue(sessionAtom)
  if (AsyncResult.isInitial(session)) return null
  if (AsyncResult.isSuccess(session) && session.value !== null) {
    return <Home key={session.value.userId} session={session.value} />
  }
  return <Login />
}

export const App = () => (
  <RegistryProvider>
    <SafeAreaView style={styles.root}>
      <StatusBar style="light" />
      <Root />
    </SafeAreaView>
  </RegistryProvider>
)

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.accent },
  fill: { flex: 1 },
  banner: { flexDirection: "row", gap: 12, padding: 10, backgroundColor: colors.warning },
  bannerText: { flex: 1 },
  bannerAction: { color: colors.accent, fontWeight: "600" }
})
