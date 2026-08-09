import { useAtomSet, useAtomValue } from "@effect/atom-react"
import * as Identity from "@lucas-barake/effect-local/Identity"
import { AsyncResult } from "effect/unstable/reactivity"
import * as React from "react"
import { Button, ScrollView, StyleSheet, Text, TextInput, View } from "react-native"
import { addLabel, createTask, labelsAtom } from "./replica"
import { runSelfTest, type SelfTestResult } from "./selfTest"

// The UI layer has no Effect runtime in scope, so command ids are minted through the
// global the polyfill installed; engine code uses Identity.makeCommandId instead.
const commandId = () => Identity.CommandId.make(`cmd_${globalThis.crypto.randomUUID()}`)

export const App = () => {
  const [results, setResults] = React.useState<ReadonlyArray<SelfTestResult>>([])
  const [title, setTitle] = React.useState("")
  const labels = useAtomValue(labelsAtom)
  const runCreateTask = useAtomSet(createTask, { mode: "promise" })
  const runAddLabel = useAtomSet(addLabel, { mode: "promise" })

  React.useEffect(() => {
    let mounted = true
    runSelfTest().then((outcome) => {
      if (mounted) setResults(outcome)
    })
    return () => {
      mounted = false
    }
  }, [])

  const failures = results.filter((result) => !result.ok)

  return (
    <ScrollView style={styles.container} contentContainerStyle={styles.content}>
      <Text style={styles.heading}>Effect Local self test</Text>
      {results.length === 0 ? <Text>Running…</Text> : null}
      {results.map((result) => (
        <Text key={result.name} style={result.ok ? styles.pass : styles.fail}>
          {result.ok ? "✓" : "✗"} {result.name}
          {result.detail ? ` — ${result.detail}` : ""}
        </Text>
      ))}
      {results.length > 0 ?
        (
          <Text style={failures.length === 0 ? styles.pass : styles.fail}>
            {failures.length === 0 ? "ALL PASS" : `${failures.length} FAILURE(S)`}
          </Text>
        ) :
        null}

      <Text style={styles.heading}>Live replica</Text>
      <View style={styles.row}>
        <TextInput style={styles.input} placeholder="Task title" value={title} onChangeText={setTitle} />
        <Button
          title="Create"
          onPress={() => {
            if (title.trim().length === 0) return
            void runCreateTask(title.trim())
            setTitle("")
          }}
        />
      </View>
      <Button
        title="Add 'errands' label to latest task"
        onPress={() => {
          if (!AsyncResult.isSuccess(labels)) return
          const latest = labels.value[labels.value.length - 1]
          if (latest === undefined) return
          void runAddLabel({ commandId: commandId(), documentId: latest.sourceDocumentId, payload: "errands" })
        }}
      />
      <Text style={styles.heading}>Labels (reactive query)</Text>
      {AsyncResult.isSuccess(labels)
        ? (
          labels.value.map((row) => <Text key={`${row.sourceDocumentId}:${row.label}`}>• {row.label}</Text>)
        )
        : <Text>Loading…</Text>}
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: "#fff" },
  content: { padding: 24, paddingTop: 64, gap: 8 },
  heading: { fontSize: 18, fontWeight: "600", marginTop: 16 },
  pass: { color: "#16803c" },
  fail: { color: "#c01c28", fontWeight: "700" },
  row: { flexDirection: "row", alignItems: "center", gap: 8 },
  input: { flex: 1, borderColor: "#999", borderWidth: 1, borderRadius: 6, padding: 8 }
})
