import { StyleSheet, Text, View } from "react-native"

export const colors = {
  accent: "#008069",
  background: "#efeae2",
  surface: "#ffffff",
  outgoing: "#d9fdd3",
  muted: "#667781",
  read: "#53bdeb",
  warning: "#fff4ce",
  border: "#e9edef"
}

export const Avatar = ({ name, color, size = 40 }: {
  readonly name: string
  readonly color: string
  readonly size?: number
}) => (
  <View style={[styles.avatar, { backgroundColor: color, width: size, height: size, borderRadius: size / 2 }]}>
    <Text style={[styles.avatarText, { fontSize: size * 0.42 }]}>{name.slice(0, 1).toUpperCase()}</Text>
  </View>
)

const styles = StyleSheet.create({
  avatar: { alignItems: "center", justifyContent: "center" },
  avatarText: { color: "#ffffff", fontWeight: "600" }
})
