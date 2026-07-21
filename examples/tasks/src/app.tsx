import { useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import {
  Check,
  Circle,
  Download,
  HardDrive,
  ListFilter,
  Pencil,
  Plus,
  Search,
  Trash2,
  Upload,
  Wifi,
  WifiOff,
  X
} from "lucide-react"
import { type ChangeEvent, type FormEvent, useEffect, useMemo, useRef, useState } from "react"
import {
  connectionStatus,
  createTask,
  deleteTask,
  exportBackup,
  renameTask,
  restoreBackup,
  setTaskCompleted,
  tasks
} from "./replica-client.ts"

const filters = ["all", "active", "completed"] as const

const TaskItem = ({
  completed,
  documentId,
  onDelete,
  onRename,
  onToggle,
  title
}: {
  readonly completed: boolean
  readonly documentId: Identity.DocumentId
  readonly onDelete: (documentId: Identity.DocumentId) => Promise<unknown>
  readonly onRename: (documentId: Identity.DocumentId, title: string) => Promise<unknown>
  readonly onToggle: (documentId: Identity.DocumentId, completed: boolean) => Promise<unknown>
  readonly title: string
}) => {
  const [editing, setEditing] = useState(false)
  const [nextTitle, setNextTitle] = useState(title)

  const save = async () => {
    const normalized = nextTitle.trim()
    if (normalized.length === 0 || normalized === title) {
      setNextTitle(title)
      setEditing(false)
      return
    }
    await onRename(documentId, normalized)
    setEditing(false)
  }

  return (
    <li className="task-row" data-task-id={documentId}>
      <button
        aria-label={completed ? `Mark ${title} active` : `Mark ${title} complete`}
        className={`icon-button complete-button${completed ? " is-complete" : ""}`}
        title={completed ? "Mark active" : "Mark complete"}
        type="button"
        onClick={() => void onToggle(documentId, !completed)}
      >
        {completed ? <Check aria-hidden="true" size={18} /> : <Circle aria-hidden="true" size={18} />}
      </button>
      {editing
        ? (
          <form
            className="edit-form"
            onSubmit={(event) => {
              event.preventDefault()
              void save()
            }}
          >
            <input
              aria-label="Task title"
              autoFocus
              maxLength={160}
              value={nextTitle}
              onChange={(event) => setNextTitle(event.target.value)}
            />
            <button aria-label="Save title" className="icon-button" title="Save" type="submit">
              <Check aria-hidden="true" size={17} />
            </button>
            <button
              aria-label="Cancel editing"
              className="icon-button"
              title="Cancel"
              type="button"
              onClick={() => {
                setNextTitle(title)
                setEditing(false)
              }}
            >
              <X aria-hidden="true" size={17} />
            </button>
          </form>
        )
        : <span className={completed ? "task-title completed" : "task-title"}>{title}</span>}
      {!editing && (
        <div className="row-actions">
          <button
            aria-label={`Rename ${title}`}
            className="icon-button"
            title="Rename"
            type="button"
            onClick={() => setEditing(true)}
          >
            <Pencil aria-hidden="true" size={16} />
          </button>
          <button
            aria-label={`Delete ${title}`}
            className="icon-button destructive"
            title="Delete"
            type="button"
            onClick={() => void onDelete(documentId)}
          >
            <Trash2 aria-hidden="true" size={16} />
          </button>
        </div>
      )}
    </li>
  )
}

export const App = () => {
  const [filter, setFilter] = useState<(typeof filters)[number]>("all")
  const [search, setSearch] = useState("")
  const [title, setTitle] = useState("")
  const [online, setOnline] = useState(navigator.onLine)
  const [message, setMessage] = useState("")
  const [notice, setNotice] = useState("")
  const [pending, setPending] = useState(0)
  const [storage, setStorage] = useState<"checking" | "persisted" | "best-effort" | "unsupported">("checking")
  const restoreInput = useRef<HTMLInputElement>(null)
  const queryAtom = useMemo(() => tasks({ filter, search }), [filter, search])
  const result = useAtomValue(queryAtom)
  const refresh = useAtomRefresh(queryAtom)
  const replicaStatus = useAtomValue(connectionStatus)
  const runCreate = useAtomSet(createTask, { mode: "promise" })
  const runRename = useAtomSet(renameTask, { mode: "promise" })
  const runCompleted = useAtomSet(setTaskCompleted, { mode: "promise" })
  const runDelete = useAtomSet(deleteTask, { mode: "promise" })
  const runExport = useAtomSet(exportBackup, { mode: "promise" })
  const runRestore = useAtomSet(restoreBackup, { mode: "promise" })

  useEffect(() => {
    const update = () => setOnline(navigator.onLine)
    window.addEventListener("online", update)
    window.addEventListener("offline", update)
    return () => {
      window.removeEventListener("online", update)
      window.removeEventListener("offline", update)
    }
  }, [])

  useEffect(() => {
    let active = true
    const requestPersistence = async () => {
      if (navigator.storage?.persist === undefined) {
        if (active) setStorage("unsupported")
        return
      }
      try {
        const persisted = await navigator.storage.persisted()
        const granted = persisted || await navigator.storage.persist()
        if (active) setStorage(granted ? "persisted" : "best-effort")
      } catch {
        if (active) setStorage("best-effort")
      }
    }
    void requestPersistence()
    return () => {
      active = false
    }
  }, [])

  const execute = async (operation: () => Promise<unknown>) => {
    setPending((value) => value + 1)
    setMessage("")
    setNotice("")
    try {
      await operation()
      refresh()
      return true
    } catch (error) {
      setMessage(error instanceof Error ? error.message : String(error))
      return false
    } finally {
      setPending((value) => value - 1)
    }
  }

  const add = async (event: FormEvent) => {
    event.preventDefault()
    const normalized = title.trim()
    if (normalized.length === 0) return
    if (await execute(() => runCreate({ title: normalized }))) setTitle("")
  }

  const downloadBackup = async () => {
    setPending((value) => value + 1)
    setMessage("")
    setNotice("")
    try {
      const bytes = new Uint8Array(await runExport(undefined))
      const url = URL.createObjectURL(new Blob([bytes.buffer], { type: "application/x-ndjson" }))
      const link = document.createElement("a")
      link.href = url
      link.download = `local-tasks-${new Date().toISOString().slice(0, 10)}.ndjson`
      link.click()
      URL.revokeObjectURL(url)
      setNotice("Backup downloaded")
    } catch (error) {
      setMessage(error instanceof Error ? error.message : String(error))
    } finally {
      setPending((value) => value - 1)
    }
  }

  const restoreFromFile = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    event.target.value = ""
    if (file === undefined) return
    if (!window.confirm("Replace all local tasks with this backup? This cannot be undone.")) return
    const bytes = new Uint8Array(await file.arrayBuffer())
    if (await execute(() => runRestore(bytes))) setNotice("Backup restored")
  }

  const rows = result._tag === "Success" ? result.value : []
  const activeCount = rows.filter((task) => !task.completed).length
  const statusText = !online
    ? "Offline, saved locally"
    : replicaStatus._tag === "Success" && replicaStatus.value._tag === "Ready"
    ? "Local replica ready"
    : "Starting local replica"

  return (
    <main>
      <header className="app-header">
        <div className="brand">
          <span className="brand-mark">
            <Check aria-hidden="true" size={20} />
          </span>
          <div>
            <h1>Local Tasks</h1>
            <p>{activeCount} {activeCount === 1 ? "task" : "tasks"} left</p>
          </div>
        </div>
        <div className="header-meta">
          <div className={`connection${online ? "" : " offline"}`} aria-live="polite">
            {online ? <Wifi aria-hidden="true" size={16} /> : <WifiOff aria-hidden="true" size={16} />}
            <span>{statusText}</span>
            {pending > 0 && <span className="saving">Saving</span>}
          </div>
          <div className="storage-status" title="Browser storage policy">
            <HardDrive aria-hidden="true" size={15} />
            <span>
              {storage === "persisted"
                ? "Persistent storage"
                : storage === "checking"
                ? "Checking storage"
                : "Best effort storage"}
            </span>
          </div>
        </div>
      </header>

      <section className="workspace" aria-label="Task manager">
        <form className="create-form" onSubmit={(event) => void add(event)}>
          <input
            aria-label="New task title"
            maxLength={160}
            placeholder="What needs doing?"
            value={title}
            onChange={(event) => setTitle(event.target.value)}
          />
          <button className="primary-button" disabled={title.trim().length === 0} type="submit">
            <Plus aria-hidden="true" size={18} />
            <span>Add task</span>
          </button>
        </form>

        <div className="task-toolbar">
          <div className="filter-control" aria-label="Task filter" role="group">
            {filters.map((value) => (
              <button
                aria-pressed={filter === value}
                className={filter === value ? "selected" : ""}
                key={value}
                type="button"
                onClick={() => setFilter(value)}
              >
                {value[0].toUpperCase() + value.slice(1)}
              </button>
            ))}
          </div>
          <label className="search-control">
            <Search aria-hidden="true" size={17} />
            <input
              aria-label="Search tasks"
              placeholder="Search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
            {search.length > 0 && (
              <button
                aria-label="Clear search"
                className="icon-button"
                title="Clear"
                type="button"
                onClick={() => setSearch("")}
              >
                <X aria-hidden="true" size={15} />
              </button>
            )}
          </label>
        </div>

        <div className="backup-toolbar">
          <span>Local backup</span>
          <div className="backup-actions">
            <button className="secondary-button" type="button" onClick={() => void downloadBackup()}>
              <Download aria-hidden="true" size={16} />
              <span>Download</span>
            </button>
            <button className="secondary-button" type="button" onClick={() => restoreInput.current?.click()}>
              <Upload aria-hidden="true" size={16} />
              <span>Restore</span>
            </button>
            <input
              ref={restoreInput}
              accept=".ndjson,application/x-ndjson,application/json"
              aria-label="Choose backup file"
              className="visually-hidden"
              type="file"
              onChange={(event) => void restoreFromFile(event)}
            />
          </div>
        </div>

        {message.length > 0 && <p className="error-message" role="alert">{message}</p>}
        {notice.length > 0 && <p className="notice-message" role="status">{notice}</p>}
        {result._tag === "Failure" && <p className="error-message" role="alert">{Cause.pretty(result.cause)}</p>}
        {result._tag === "Initial" && <div className="loading-row">Opening local database</div>}
        {result._tag === "Success" && rows.length === 0 && (
          <div className="empty-state">
            <ListFilter aria-hidden="true" size={24} />
            <strong>{search.length > 0 || filter !== "all" ? "No matching tasks" : "No tasks yet"}</strong>
            <span>{search.length > 0 || filter !== "all" ? "Try another view" : "Add your first task above"}</span>
          </div>
        )}
        {rows.length > 0 && (
          <ul className="task-list">
            {rows.map((task) => (
              <TaskItem
                completed={task.completed}
                documentId={task.sourceDocumentId}
                key={task.sourceDocumentId}
                title={task.title}
                onDelete={(documentId) => execute(() => runDelete({ documentId }))}
                onRename={(documentId, nextTitle) => execute(() => runRename({ documentId, title: nextTitle }))}
                onToggle={(documentId, completed) => execute(() => runCompleted({ completed, documentId }))}
              />
            ))}
          </ul>
        )}
      </section>
    </main>
  )
}
