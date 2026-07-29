import { useAtomMount, useAtomSet, useAtomValue } from "@effect/atom-react"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import { useEffect } from "react"
import * as Device from "./device.ts"

declare global {
  interface Window {
    relayFixture?: {
      readonly createTask: (title: string) => Promise<Identity.DocumentId>
      readonly adoptTask: (documentId: Identity.DocumentId) => void
      readonly addLabel: (input: {
        readonly documentId: Identity.DocumentId
        readonly label: string
      }) => Promise<void>
      readonly readTask: (
        documentId: Identity.DocumentId
      ) => Promise<{ title: string; labels: Array<string> }>
      readonly exportBackup: () => Promise<Array<number>>
      readonly restoreBackup: (bytes: ReadonlyArray<number>) => Promise<void>
      readonly push: (documentId: Identity.DocumentId) => Promise<void>
    }
  }
}

export const App = () => {
  useAtomMount(Device.sessionAtom)
  const connectionStatus = useAtomValue(Device.peerConnectionStatus)

  const createTask = useAtomSet(Device.createTask, { mode: "promise" })
  const adoptTask = useAtomSet(Device.syncDocument)
  const addLabel = useAtomSet(Device.addLabel, { mode: "promise" })
  const readTask = useAtomSet(Device.readTask, { mode: "promise" })
  const exportBackup = useAtomSet(Device.exportBackup, { mode: "promise" })
  const restoreBackup = useAtomSet(Device.restoreBackup, { mode: "promise" })
  const push = useAtomSet(Device.push, { mode: "promise" })

  useEffect(() => {
    window.relayFixture = {
      createTask,
      adoptTask,
      addLabel,
      readTask,
      exportBackup,
      restoreBackup,
      push
    }
    document.body.dataset.ready = "true"
  }, [createTask, adoptTask, addLabel, readTask, exportBackup, restoreBackup, push])

  useEffect(() => {
    document.body.dataset.connectionStatus = connectionStatus._tag === "Success"
      ? connectionStatus.value._tag
      : connectionStatus._tag
  }, [connectionStatus])

  return null
}
