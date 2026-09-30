import { type UserId, users } from "@effect-local/example-chat-shared/domain"
import { useAtomSet, useAtomValue } from "@effect/atom-react"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import { useState } from "react"
import { Pressable, ScrollView, StyleSheet, Text, TextInput, View } from "react-native"
import { loginAtom } from "./runtime.js"
import { Avatar, colors } from "./theme.js"

export const Login = () => {
  const [userId, setUserId] = useState<UserId>(users[0].id)
  const [password, setPassword] = useState("")
  const login = useAtomSet(loginAtom)
  const result = useAtomValue(loginAtom)
  const error = AsyncResult.matchWithError(result, {
    onInitial: () => undefined,
    onSuccess: () => undefined,
    onError: (failure) => failure.message,
    onDefect: () => "Sign in failed unexpectedly. Try again."
  })
  const submit = () => login({ username: userId, password })

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled">
      <View style={styles.card}>
        <Text style={styles.title}>Effect Chat</Text>
        <Text style={styles.subtitle}>Local-first chat demo. Pick a user and sign in.</Text>
        <View style={styles.users}>
          {users.map((user) => (
            <Pressable
              key={user.id}
              accessibilityRole="button"
              accessibilityState={{ selected: user.id === userId }}
              onPress={() => setUserId(user.id)}
              style={[styles.user, user.id === userId && styles.userActive]}
            >
              <Avatar name={user.name} color={user.color} size={32} />
              <Text style={styles.userName}>{user.name}</Text>
            </Pressable>
          ))}
        </View>
        <TextInput
          style={styles.input}
          value={password}
          onChangeText={setPassword}
          placeholder={`Password (hint: ${userId}123)`}
          secureTextEntry
          autoCapitalize="none"
          onSubmitEditing={submit}
        />
        <Pressable accessibilityRole="button" style={styles.submit} onPress={submit}>
          <Text style={styles.submitText}>Sign in</Text>
        </Pressable>
        {error !== undefined && <Text accessibilityRole="alert" style={styles.error}>{error}</Text>}
      </View>
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: colors.background },
  content: { flexGrow: 1, justifyContent: "center", padding: 24 },
  card: { backgroundColor: colors.surface, borderRadius: 16, padding: 24, gap: 12 },
  title: { fontSize: 26, fontWeight: "700", color: colors.accent },
  subtitle: { color: colors.muted },
  users: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  user: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    paddingVertical: 6,
    paddingHorizontal: 10,
    borderRadius: 20,
    borderWidth: 1,
    borderColor: colors.border
  },
  userActive: { borderColor: colors.accent, backgroundColor: colors.outgoing },
  userName: { fontSize: 15 },
  input: { borderWidth: 1, borderColor: colors.border, borderRadius: 8, padding: 12, fontSize: 16 },
  submit: { backgroundColor: colors.accent, borderRadius: 8, padding: 14, alignItems: "center" },
  submitText: { color: "#ffffff", fontWeight: "600", fontSize: 16 },
  error: { color: "#c0392b" }
})
