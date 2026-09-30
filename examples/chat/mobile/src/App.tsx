import type { LoginResponse } from "@effect-local/example-chat-shared/auth"
import type { Connection } from "@effect-local/example-chat-shared/connection"
import { type ConversationId, groupConversationId, UserId } from "@effect-local/example-chat-shared/domain"
import { RegistryProvider, useAtomMount, useAtomSet, useAtomValue } from "@effect/atom-react"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import { StatusBar } from "expo-status-bar"
import { useEffect, useState } from "react"
import { Keyboard, KeyboardAvoidingView, Pressable, StyleSheet, Text, View } from "react-native"
import { SafeAreaProvider, SafeAreaView } from "react-native-safe-area-context"
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
  const renew = useAtomSet(client.renewCredential)
  useEffect(() => renew(session.token), [renew, session.token])
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

const useKeyboardVisible = () => {
  const [visible, setVisible] = useState(false)
  useEffect(() => {
    const shown = Keyboard.addListener("keyboardDidShow", () => setVisible(true))
    const hidden = Keyboard.addListener("keyboardDidHide", () => setVisible(false))
    return () => {
      shown.remove()
      hidden.remove()
    }
  }, [])
  return visible
}

const Screen = () => {
  const keyboardVisible = useKeyboardVisible()
  return (
    <KeyboardAvoidingView style={styles.fill} behavior="padding">
      <SafeAreaView
        style={styles.root}
        edges={keyboardVisible ? ["top", "left", "right"] : ["top", "left", "right", "bottom"]}
      >
        <StatusBar style="light" />
        <Root />
      </SafeAreaView>
    </KeyboardAvoidingView>
  )
}

export const App = () => (
  <RegistryProvider>
    <SafeAreaProvider>
      <Screen />
    </SafeAreaProvider>
  </RegistryProvider>
)

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.accent },
  fill: { flex: 1 },
  banner: { flexDirection: "row", gap: 12, padding: 10, backgroundColor: colors.warning },
  bannerText: { flex: 1 },
  bannerAction: { color: colors.accent, fontWeight: "600" }
})
