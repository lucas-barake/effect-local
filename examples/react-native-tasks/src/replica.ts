import * as AppLifecycle from "@lucas-barake/effect-local-react-native/AppLifecycle"
import * as ExpoSqlite from "@lucas-barake/effect-local-react-native/ExpoSqlite"
import * as ReactNativeCrypto from "@lucas-barake/effect-local-react-native/ReactNativeCrypto"
import * as ReactNativeReplica from "@lucas-barake/effect-local-react-native/ReactNativeReplica"
import * as ReplicaAtom from "@lucas-barake/effect-local-react-native/ReplicaAtom"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { Atom } from "effect/unstable/reactivity"
import { AddLabel, AddLabelLive, definition, LabelsSql, limits, ListLabels, ListLabelsLive, Task } from "./domain"

const Database = ExpoSqlite.layer({ databaseName: "tasks.db" })

const Dependencies = Layer.mergeAll(
  Database,
  ReactNativeCrypto.layer,
  ReplicaLimits.layer(limits)
)

const DomainLive = Layer.mergeAll(
  AddLabelLive,
  ListLabelsLive.pipe(Layer.provide(Database))
)

// provideMerge keeps SqlClient, Crypto, and ReplicaLimits in the output context so atom
// functions can reach them. The lifecycle layer is provided with the replica graph it
// flushes (Layer.merge does not feed outputs between siblings); the memo map builds the
// graph once either way. AppLifecycle flushes on background so committed mutations are
// durable before the OS can suspend the process.
const replicaGraph = ReactNativeReplica.layer(definition, { projections: [LabelsSql] })

export const ReplicaLive = Layer.merge(
  replicaGraph,
  AppLifecycle.layerFlushOnBackground.pipe(Layer.provide(replicaGraph))
).pipe(
  Layer.provide(DomainLive),
  Layer.provideMerge(Dependencies)
)

export const runtime = Atom.runtime(ReplicaLive)

export const labelsAtom = ReplicaAtom.queryFamily(runtime, ListLabels)({ prefix: "" })

export const createTask = runtime.fn<string>()(
  Effect.fnUntraced(function*(title) {
    const replica = yield* Replica.Replica
    return yield* replica.create(Task, {
      commandId: yield* Identity.makeCommandId,
      value: { title, labels: [] }
    })
  })
)

export const addLabel = ReplicaAtom.mutation(runtime, AddLabel)
