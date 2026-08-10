import { useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
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
  X
} from "lucide-react"
import { type ChangeEvent, type FormEvent, useEffect, useMemo, useRef, useState } from "react"
import { ListTasks, type TaskRow } from "./domain.ts"
import {
  createTask,
  deleteTask,
  exportBackup,
  renameTask,
  replicaStatus,
  restoreBackup,
  setTaskCompleted,
  tasks
} from "./replica-client.ts"

const filters: ReadonlyArray<"all" | "active" | "completed"> = ["all", "active", "completed"]

const filterClassName = (active: boolean) => {
  if (active) return "selected"
  return ""
}

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

  const save = () => {
    const normalized = nextTitle.trim()
    if (normalized.length === 0 || normalized === title) {
      setNextTitle(title)
      setEditing(false)
      return
    }
    void Effect.runPromise(Effect.gen(function*() {
      yield* Effect.tryPromise(() => onRename(documentId, normalized))
      setEditing(false)
    }))
  }

  let actionLabel = "complete"
  let completionClass = ""
  let titleClass = "task-title"
  if (completed) {
    actionLabel = "active"
    completionClass = " is-complete"
    titleClass = "task-title completed"
  }

  return (
    <li className="task-row" data-task-id={documentId}>
      <button
        aria-label={`Mark ${title} ${actionLabel}`}
        className={`icon-button complete-button${completionClass}`}
        title={`Mark ${actionLabel}`}
        type="button"
        onClick={() => void onToggle(documentId, !completed)}
      >
        {completed && <Check aria-hidden="true" size={18} />}
        {!completed && <Circle aria-hidden="true" size={18} />}
      </button>
      {editing && (
        <form
          className="edit-form"
          onSubmit={(event) => {
            event.preventDefault()
            save()
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
      )}
      {!editing && <span className={titleClass}>{title}</span>}
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
  const [message, setMessage] = useState("")
  const [notice, setNotice] = useState("")
  const [pending, setPending] = useState(0)
  const [storage, setStorage] = useState<"checking" | "persisted" | "best-effort" | "unsupported">("checking")
  const restoreInput = useRef<HTMLInputElement>(null)
  const queryAtom = useMemo(() => tasks({ filter, search }), [filter, search])
  const result = useAtomValue(queryAtom)
  const refresh = useAtomRefresh(queryAtom)
  const currentReplicaStatus = useAtomValue(replicaStatus)
  const runCreate = useAtomSet(createTask, { mode: "promise" })
  const runRename = useAtomSet(renameTask, { mode: "promise" })
  const runCompleted = useAtomSet(setTaskCompleted, { mode: "promise" })
  const runDelete = useAtomSet(deleteTask, { mode: "promise" })
  const runExport = useAtomSet(exportBackup, { mode: "promise" })
  const runRestore = useAtomSet(restoreBackup, { mode: "promise" })

  useEffect(() => {
    let active = true
    const requestPersistence = Effect.gen(function*() {
      const storageApi = navigator.storage
      if (storageApi?.persist === undefined) {
        if (active) setStorage("unsupported")
        return
      }
      let granted = yield* Effect.tryPromise(() => storageApi.persisted())
      if (!granted) granted = yield* Effect.tryPromise(() => storageApi.persist())
      if (active) {
        if (granted) {
          setStorage("persisted")
        } else {
          setStorage("best-effort")
        }
      }
    }).pipe(Effect.catch(() =>
      Effect.sync(() => {
        if (active) setStorage("best-effort")
      })
    ))
    void Effect.runPromise(requestPersistence)
    return () => {
      active = false
    }
  }, [])

  const runOperation = <A,>(operation: Effect.Effect<A, unknown>, onSuccess: (value: A) => void) => {
    setPending((value) => value + 1)
    setMessage("")
    setNotice("")
    const program = Effect.gen(function*() {
      const exit = yield* Effect.exit(operation)
      if (exit._tag === "Failure") {
        setMessage(Cause.pretty(exit.cause))
        return false
      }
      onSuccess(exit.value)
      return true
    }).pipe(Effect.ensuring(Effect.sync(() => {
      setPending((value) => value - 1)
    })))
    return Effect.runPromise(program)
  }

  const execute = (operation: Effect.Effect<unknown, unknown>) => runOperation(operation, () => refresh())

  const add = (event: FormEvent) => {
    event.preventDefault()
    const normalized = title.trim()
    if (normalized.length === 0) return undefined
    return Effect.runPromise(Effect.gen(function*() {
      const succeeded = yield* Effect.tryPromise(() =>
        execute(Effect.tryPromise(() => runCreate({ title: normalized })))
      )
      if (succeeded) setTitle("")
    }))
  }

  const downloadBackup = () => {
    const operation = Effect.gen(function*() {
      const value = yield* Effect.tryPromise(() => runExport(undefined))
      const bytes = new Uint8Array(value)
      const url = URL.createObjectURL(new Blob([bytes.buffer], { type: "application/x-ndjson" }))
      const link = document.createElement("a")
      link.href = url
      const date = yield* DateTime.now
      link.download = `local-tasks-${DateTime.formatIsoDate(date)}.ndjson`
      link.click()
      URL.revokeObjectURL(url)
    })
    return runOperation(operation, () => setNotice("Backup downloaded"))
  }

  const restoreFromFile = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    event.target.value = ""
    if (file === undefined) return undefined
    if (!window.confirm("Replace all local tasks with this backup? This cannot be undone.")) return undefined
    const operation = Effect.gen(function*() {
      const value = yield* Effect.tryPromise(() => file.arrayBuffer())
      yield* Effect.tryPromise(() => runRestore(new Uint8Array(value)))
    })
    return runOperation(operation, () => setNotice("Backup restored"))
  }

  const rows: ReadonlyArray<TaskRow> = (() => {
    if (result._tag !== "Success") return []
    return Schema.decodeUnknownSync(ListTasks.successSchema)(result.value)
  })()
  const activeCount = rows.filter((task) => !task.completed).length
  let statusText = "Starting local replica"
  if (currentReplicaStatus._tag === "Success" && currentReplicaStatus.value._tag === "Ready") {
    statusText = "Local replica ready"
  } else if (currentReplicaStatus._tag === "Success" && currentReplicaStatus.value._tag === "Degraded") {
    statusText = `Local replica degraded: ${currentReplicaStatus.value.reason}`
  }
  let taskCountLabel = "tasks"
  if (activeCount === 1) taskCountLabel = "task"
  let storageLabel = "Best effort storage"
  if (storage === "persisted") storageLabel = "Persistent storage"
  if (storage === "checking") storageLabel = "Checking storage"
  let noTasksLabel = "No tasks yet"
  let noTasksHint = "Add your first task above"
  if (search.length > 0 || filter !== "all") {
    noTasksLabel = "No matching tasks"
    noTasksHint = "Try another view"
  }

  return (
    <main>
      <header className="app-header">
        <div className="brand">
          <span className="brand-mark">
            <Check aria-hidden="true" size={20} />
          </span>
          <div>
            <h1>Local Tasks</h1>
            <p>{activeCount} {taskCountLabel} left</p>
          </div>
        </div>
        <div className="header-meta">
          <div className="connection" aria-live="polite">
            <Wifi aria-hidden="true" size={16} />
            <span>{statusText}</span>
            {pending > 0 && <span className="saving">Saving</span>}
          </div>
          <div className="storage-status" title="Browser storage policy">
            <HardDrive aria-hidden="true" size={15} />
            <span>
              {storageLabel}
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
                className={filterClassName(filter === value)}
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
            <strong>{noTasksLabel}</strong>
            <span>{noTasksHint}</span>
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
                onDelete={(documentId) => execute(Effect.tryPromise(() => runDelete({ documentId })))}
                onRename={(documentId, nextTitle) =>
                  execute(Effect.tryPromise(() => runRename({ documentId, title: nextTitle })))}
                onToggle={(documentId, completed) =>
                  execute(Effect.tryPromise(() => runCompleted({ completed, documentId })))}
              />
            ))}
          </ul>
        )}
      </section>
    </main>
  )
}
