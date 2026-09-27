import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Match from "effect/Match"
import * as Stream from "effect/Stream"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"

export type Connection = "online" | "connecting" | "idle" | "offline" | "needsAuthentication" | "failed"

export const connecting: Connection = "connecting"

const connectionOf = (status: AsyncResult.AsyncResult<ReplicaStatus.SpaceStatus, unknown>): Connection => {
  if (AsyncResult.isInitial(status)) return connecting
  if (AsyncResult.isFailure(status)) return "failed"
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
  statuses: Stream.Stream<AsyncResult.AsyncResult<ReplicaStatus.SpaceStatus, unknown>, E, R>
): Stream.Stream<Connection, E, R> => statuses.pipe(Stream.map(connectionOf), Stream.changes)
