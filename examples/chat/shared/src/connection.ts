import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import * as Stream from "effect/Stream"

export type Connection =
  | "online"
  | "connecting"
  | "idle"
  | "offline"
  | "needsAuthentication"
  | "failed"
  | "superseded"

export const connecting: Connection = "connecting"

type StatusError = ReplicaError.ReplicaError | { readonly _tag: string }

const connectionOf = (status: AsyncResult.AsyncResult<ReplicaStatus.SpaceStatus, StatusError>): Connection => {
  if (AsyncResult.isInitial(status)) return connecting
  if (AsyncResult.isFailure(status)) {
    if (Option.exists(AsyncResult.error(status), (error) => error._tag === "BuildSuperseded")) return "superseded"
    return "failed"
  }
  return Match.value(status.value).pipe(
    Match.tagsExhaustive({
      Online: (): Connection => "online",
      SchemaUpdateAvailable: (): Connection => "online",
      NeedsAuthentication: (): Connection => "needsAuthentication",
      Failed: (): Connection => "failed",
      Offline: (): Connection => "offline",
      Connecting: (): Connection => connecting,
      Idle: (): Connection => "idle"
    })
  )
}

export const connectionChanges = <E, R,>(
  statuses: Stream.Stream<AsyncResult.AsyncResult<ReplicaStatus.SpaceStatus, StatusError>, E, R>
): Stream.Stream<Connection, E, R> => statuses.pipe(Stream.map(connectionOf), Stream.changes)
