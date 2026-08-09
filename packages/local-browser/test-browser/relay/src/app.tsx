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
      readonly makeCommandId: () => Promise<Identity.CommandId>
      readonly probeInteractiveDatabase: (commandId: Identity.CommandId) => Promise<void>
      readonly exportBackup: () => Promise<Array<number>>
      readonly restoreBackup: (bytes: ReadonlyArray<number>) => Promise<void>
      readonly push: (documentId: Identity.DocumentId) => Promise<void>
      readonly runBackgroundDatabaseOperation: (label: string) => Promise<void>
      readonly waitForDatabaseOperation: (label: string) => Promise<void>
      readonly waitForInteractiveReservation: () => Promise<void>
      readonly waitForNoInteractiveReservations: () => Promise<void>
      readonly waitForBackgroundReservations: (minimum: number) => Promise<void>
      readonly armNextDatabaseResponse: () => void
      readonly waitForDatabaseRequest: () => Promise<void>
      readonly waitForDatabaseResponse: () => Promise<void>
      readonly nextDatabaseRequest: () => Promise<unknown>
      readonly releaseDatabaseResponse: () => void
    }
  }
}

export const App = () => {
  useAtomMount(Device.sessionAtom)
  const connectionStatus = useAtomValue(Device.peerConnectionStatus)
  const relayStatus = useAtomValue(Device.relayConnectionStatus)

  const createTask = useAtomSet(Device.createTask, { mode: "promise" })
  const adoptTask = useAtomSet(Device.syncDocument)
  const addLabel = useAtomSet(Device.addLabel, { mode: "promise" })
  const readTask = useAtomSet(Device.readTask, { mode: "promise" })
  const makeCommandId = useAtomSet(Device.makeCommandId, { mode: "promise" })
  const probeInteractiveDatabase = useAtomSet(Device.probeInteractiveDatabase, { mode: "promise" })
  const exportBackup = useAtomSet(Device.exportBackup, { mode: "promise" })
  const restoreBackup = useAtomSet(Device.restoreBackup, { mode: "promise" })
  const push = useAtomSet(Device.push, { mode: "promise" })
  const runBackgroundDatabaseOperation = useAtomSet(Device.runBackgroundDatabaseOperation, { mode: "promise" })
  const waitForInteractiveReservation = useAtomSet(Device.waitForInteractiveReservation, { mode: "promise" })
  const waitForNoInteractiveReservations = useAtomSet(Device.waitForNoInteractiveReservations, { mode: "promise" })
  const waitForBackgroundReservations = useAtomSet(Device.waitForBackgroundReservations, { mode: "promise" })

  useEffect(() => {
    window.relayFixture = {
      createTask,
      adoptTask,
      addLabel,
      readTask,
      makeCommandId,
      probeInteractiveDatabase,
      exportBackup,
      restoreBackup,
      push,
      runBackgroundDatabaseOperation,
      waitForDatabaseOperation: Device.waitForDatabaseOperation,
      waitForInteractiveReservation,
      waitForNoInteractiveReservations,
      waitForBackgroundReservations,
      armNextDatabaseResponse: Device.databaseBridge.arm,
      waitForDatabaseRequest: Device.databaseBridge.waitForRequest,
      waitForDatabaseResponse: Device.databaseBridge.waitForResponse,
      nextDatabaseRequest: Device.databaseBridge.nextRequest,
      releaseDatabaseResponse: Device.databaseBridge.release
    }
    document.body.dataset.ready = "true"
  }, [
    createTask,
    adoptTask,
    addLabel,
    readTask,
    makeCommandId,
    probeInteractiveDatabase,
    exportBackup,
    restoreBackup,
    push,
    runBackgroundDatabaseOperation,
    waitForInteractiveReservation,
    waitForNoInteractiveReservations,
    waitForBackgroundReservations
  ])

  useEffect(() => {
    document.body.dataset.connectionStatus = connectionStatus._tag === "Success"
      ? connectionStatus.value._tag
      : connectionStatus._tag
  }, [connectionStatus])

  // Separate from the peer status above, and deliberately so: every peer session runs over this one
  // socket, so a relay drop takes them all quiet at once and per peer status alone would read as
  // several peers vanishing rather than as one link failing.
  useEffect(() => {
    document.body.dataset.relayStatus = relayStatus._tag === "Success"
      ? relayStatus.value._tag
      : relayStatus._tag
  }, [relayStatus])

  return null
}
