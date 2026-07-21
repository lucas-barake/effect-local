/// <reference lib="webworker" />
import { BrowserWorkerRunner } from "@effect/platform-browser"
import { SqliteClient } from "@effect/sql-sqlite-wasm"
import { Effect, Exit, Layer, Option, Schema, Stream } from "effect"
import { ClusterWorkflowEngine, SingleRunner } from "effect/unstable/cluster"
import { RpcServer } from "effect/unstable/rpc"
import { SqlClient, SqlSchema } from "effect/unstable/sql"
import { Activity, DurableClock } from "effect/unstable/workflow"
import { CommandResult, DocumentEntity, PageApi, RecoveryWorkflow } from "./schema.ts"

const diagnostics = new BroadcastChannel("effect-local-stage0-diagnostics")
diagnostics.postMessage("shared worker loaded")

const ApplicationSchemaLive = Layer.effectDiscard(Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`
    CREATE TABLE IF NOT EXISTS document_events (
      command_id TEXT PRIMARY KEY,
      document_id TEXT NOT NULL,
      revision INTEGER NOT NULL,
      value TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )
  `
  yield* sql`
    CREATE TABLE IF NOT EXISTS workflow_steps (
      execution_id TEXT NOT NULL,
      step TEXT NOT NULL,
      PRIMARY KEY (execution_id, step)
    )
  `
  diagnostics.postMessage("application schema ready")
}))

const DocumentEntityLive = DocumentEntity.toLayer(Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const insert = SqlSchema.findOne({
    Request: Schema.Struct({
      commandId: Schema.String,
      documentId: Schema.String,
      value: Schema.String
    }),
    Result: CommandResult,
    execute: ({ commandId, documentId, value }) =>
      sql`
      INSERT INTO document_events (command_id, document_id, revision, value, created_at)
      SELECT
        ${commandId},
        ${documentId},
        COALESCE(MAX(revision), 0) + 1,
        ${value},
        ${Date.now()}
      FROM document_events
      WHERE document_id = ${documentId}
      RETURNING
        command_id AS commandId,
        document_id AS documentId,
        revision,
        value
    `
  })

  diagnostics.postMessage("entity handlers ready")
  return DocumentEntity.of({
    Commit: ({ payload }) => insert(payload).pipe(Effect.orDie),
    Rollback: ({ payload }) =>
      Effect.gen(function*() {
        yield* sql`
          CREATE TRIGGER stage0_rollback_reply
          BEFORE INSERT ON cluster_replies
          WHEN EXISTS (
            SELECT 1
            FROM cluster_messages
            WHERE request_id = NEW.request_id
              AND tag = 'Rollback'
          )
          BEGIN
            SELECT RAISE(ABORT, 'intentional Stage 0 rollback');
          END
        `
        return yield* insert(payload)
      }).pipe(Effect.orDie)
  })
}))

const RecoveryWorkflowLive = RecoveryWorkflow.toLayer(Effect.fnUntraced(function*({ id }) {
  const sql = yield* SqlClient.SqlClient
  yield* Activity.make({
    name: "Begin",
    execute: sql`INSERT OR IGNORE INTO workflow_steps (execution_id, step) VALUES (${id}, 'begin')`.pipe(
      Effect.orDie,
      Effect.asVoid
    )
  })
  yield* DurableClock.sleep({
    name: "RestartWindow",
    duration: 8000,
    inMemoryThreshold: 0
  })
  yield* Activity.make({
    name: "Complete",
    execute: sql`INSERT OR IGNORE INTO workflow_steps (execution_id, step) VALUES (${id}, 'complete')`.pipe(
      Effect.orDie,
      Effect.asVoid
    )
  })
  return id
})).pipe(
  Layer.provideMerge(ClusterWorkflowEngine.layer)
)

const ClusterLive = Layer.merge(DocumentEntityLive, RecoveryWorkflowLive).pipe(
  Layer.provideMerge(SingleRunner.layer({ runnerStorage: "sql" }))
)

const PageHandlersLive = PageApi.toLayer(Effect.gen(function*() {
  const document = yield* DocumentEntity.client
  const sql = yield* SqlClient.SqlClient

  const inspect = SqlSchema.findOne({
    Request: Schema.String,
    Result: Schema.Struct({
      eventCount: Schema.Number,
      latestValue: Schema.String,
      processedCount: Schema.Number,
      replyCount: Schema.Number,
      storedReplyPayload: Schema.String
    }),
    execute: (commandId) =>
      sql`
      SELECT
        (SELECT COUNT(*) FROM document_events WHERE command_id = ${commandId}) AS eventCount,
        COALESCE((SELECT value FROM document_events WHERE command_id = ${commandId}), '') AS latestValue,
        (
          SELECT COUNT(*)
          FROM cluster_messages
          WHERE message_id =
            'Stage0Document/' ||
            (SELECT document_id FROM document_events WHERE command_id = ${commandId}) ||
            '/Commit/' || ${commandId}
            AND processed = 1
        ) AS processedCount,
        (
          SELECT COUNT(*)
          FROM cluster_replies replies
          JOIN cluster_messages messages ON messages.request_id = replies.request_id
          WHERE messages.message_id =
            'Stage0Document/' ||
            (SELECT document_id FROM document_events WHERE command_id = ${commandId}) ||
            '/Commit/' || ${commandId}
            AND replies.kind = 0
        ) AS replyCount,
        COALESCE((
          SELECT replies.payload
          FROM cluster_replies replies
          JOIN cluster_messages messages ON messages.request_id = replies.request_id
          WHERE messages.message_id =
            'Stage0Document/' ||
            (SELECT document_id FROM document_events WHERE command_id = ${commandId}) ||
            '/Commit/' || ${commandId}
            AND replies.kind = 0
          LIMIT 1
        ), '') AS storedReplyPayload
    `
  })

  const stress = SqlSchema.findOne({
    Request: Schema.Number,
    Result: Schema.Struct({ total: Schema.Number }),
    execute: (iterations) =>
      sql`
      WITH RECURSIVE counter(value) AS (
        SELECT 1
        UNION ALL
        SELECT value + 1 FROM counter WHERE value < ${iterations}
      )
      SELECT SUM(value) AS total FROM counter
    `
  })

  const inspectRollback = SqlSchema.findOne({
    Request: Schema.Struct({
      commandId: Schema.String,
      messageId: Schema.String
    }),
    Result: Schema.Struct({
      eventCount: Schema.Number,
      messageCount: Schema.Number,
      processedCount: Schema.Number,
      replyCount: Schema.Number,
      successfulReplyCount: Schema.Number,
      triggerCount: Schema.Number
    }),
    execute: ({ commandId, messageId }) =>
      sql`
      SELECT
        (SELECT COUNT(*) FROM document_events WHERE command_id = ${commandId}) AS eventCount,
        (SELECT COUNT(*) FROM cluster_messages WHERE message_id = ${messageId}) AS messageCount,
        (
          SELECT COUNT(*)
          FROM cluster_messages
          WHERE message_id = ${messageId}
            AND processed = 1
        ) AS processedCount,
        (
          SELECT COUNT(*)
          FROM cluster_replies replies
          JOIN cluster_messages messages ON messages.request_id = replies.request_id
          WHERE messages.message_id = ${messageId}
        ) AS replyCount,
        (
          SELECT COUNT(*)
          FROM cluster_replies replies
          JOIN cluster_messages messages ON messages.request_id = replies.request_id
          WHERE messages.message_id = ${messageId}
            AND replies.kind = 0
            AND replies.payload LIKE '%"Success"%'
        ) AS successfulReplyCount,
        (
          SELECT COUNT(*)
          FROM sqlite_master
          WHERE type = 'trigger'
            AND name = 'stage0_rollback_reply'
        ) AS triggerCount
    `
  })

  const inspectWorkflow = SqlSchema.findOne({
    Request: Schema.String,
    Result: Schema.Struct({
      beginCount: Schema.Number,
      completeCount: Schema.Number
    }),
    execute: (executionId) =>
      sql`
      SELECT
        (SELECT COUNT(*) FROM workflow_steps WHERE execution_id = ${executionId} AND step = 'begin') AS beginCount,
        (SELECT COUNT(*) FROM workflow_steps WHERE execution_id = ${executionId} AND step = 'complete') AS completeCount
    `
  })

  diagnostics.postMessage("page handlers ready")
  return PageApi.of({
    CommitDocument: ({ commandId, documentId, value }) =>
      document(documentId).Commit({ commandId, documentId, value }).pipe(Effect.orDie),
    InspectCommand: ({ commandId }) =>
      inspect(commandId).pipe(
        Effect.map((snapshot) => ({ commandId, ...snapshot })),
        Effect.orDie
      ),
    RollbackDocument: ({ commandId, documentId, value }) =>
      document(documentId).Rollback({ commandId, documentId, value }).pipe(Effect.orDie),
    InspectRollback: ({ commandId, documentId }) =>
      inspectRollback({
        commandId,
        messageId: `Stage0Document/${documentId}/Rollback/${commandId}`
      }).pipe(
        Effect.map((snapshot) => ({ commandId, ...snapshot })),
        Effect.orDie
      ),
    CleanupRollback: ({ commandId, documentId }) =>
      Effect.gen(function*() {
        yield* sql`DROP TRIGGER IF EXISTS stage0_rollback_reply`
        yield* sql`
          DELETE FROM cluster_messages
          WHERE message_id = ${`Stage0Document/${documentId}/Rollback/${commandId}`}
        `
      }).pipe(
        sql.withTransaction,
        Effect.orDie
      ),
    StressDatabase: Effect.fnUntraced(function*({ iterations }) {
      const startedAt = Date.now()
      const { total } = yield* stress(iterations)
      yield* Effect.sleep(350)
      return { finishedAt: Date.now(), startedAt, total }
    }, Effect.orDie),
    StartWorkflow: ({ id }) => RecoveryWorkflow.execute({ id }, { discard: true }),
    InspectWorkflow: Effect.fnUntraced(function*({ executionId, id }) {
      const counts = yield* inspectWorkflow(id)
      const result = yield* RecoveryWorkflow.poll(executionId)
      const status = Option.isNone(result)
        ? "Pending"
        : result.value._tag === "Suspended"
        ? "Suspended"
        : Exit.isSuccess(result.value.exit)
        ? "Complete"
        : "Failed"
      return { executionId, status, ...counts }
    }, Effect.orDie),
    Heartbeat: ({ count, intervalMs }) =>
      Stream.range(0, count - 1).pipe(
        Stream.mapEffect((index) =>
          Effect.sleep(intervalMs).pipe(
            Effect.as({ emittedAt: Date.now(), index })
          )
        )
      )
  })
}))

declare const self: SharedWorkerGlobalScope

self.onconnect = (connectEvent) => {
  const controlPort = connectEvent.ports[0]
  controlPort.addEventListener("message", (bootstrapEvent) => {
    const { databasePort, rpcPort } = bootstrapEvent.data as {
      readonly databasePort: MessagePort
      readonly rpcPort: MessagePort
    }
    databasePort.start()
    rpcPort.start()

    const DatabaseLive = SqliteClient.layer({
      worker: Effect.acquireRelease(
        Effect.succeed(databasePort),
        (port) => Effect.sync(() => port.close())
      )
    })

    const EngineLive = Layer.mergeAll(
      ApplicationSchemaLive,
      ClusterLive
    ).pipe(Layer.provideMerge(DatabaseLive))

    const MainLive = RpcServer.layer(PageApi).pipe(
      Layer.provide(PageHandlersLive),
      Layer.provide(RpcServer.layerProtocolWorkerRunner),
      Layer.provide(BrowserWorkerRunner.layerMessagePort(rpcPort)),
      Layer.provide(EngineLive)
    )

    Effect.runFork(
      Layer.launch(MainLive).pipe(
        Effect.tapCause((cause) =>
          Effect.sync(() => diagnostics.postMessage(`shared worker failure: ${String(cause)}`))
        ),
        Effect.ensuring(Effect.sync(() => {
          controlPort.close()
          diagnostics.close()
          self.close()
        }))
      )
    )
  }, { once: true })
  controlPort.start()
}
