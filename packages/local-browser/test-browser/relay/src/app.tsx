import { useAtomMount, useAtomSet, useAtomValue } from "@effect/atom-react"
import type * as ReplicaOperationScheduler from "@lucas-barake/effect-local-sql/ReplicaOperationScheduler"
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
      readonly waitForReservations: (
        matches: (reservations: ReplicaOperationScheduler.Reservations) => boolean
      ) => Promise<void>
      readonly armNextDatabaseResponse: () => void
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
  const waitForReservations = useAtomSet(Device.waitForReservations, { mode: "promise" })

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
      waitForReservations,
      armNextDatabaseResponse: Device.databaseBridge.arm,
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
    waitForReservations
  ])

  useEffect(() => {
    let status: string = connectionStatus._tag
    if (connectionStatus._tag === "Success") status = connectionStatus.value._tag
    document.body.dataset.connectionStatus = status
  }, [connectionStatus])

  // Separate from the peer status above, and deliberately so: every peer session runs over this one
  // socket, so a relay drop takes them all quiet at once and per peer status alone would read as
  // several peers vanishing rather than as one link failing.
  useEffect(() => {
    let status: string = relayStatus._tag
    if (relayStatus._tag === "Success") status = relayStatus.value._tag
    document.body.dataset.relayStatus = status
  }, [relayStatus])

  return null
}
