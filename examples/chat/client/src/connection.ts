import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Match from "effect/Match"
import * as Stream from "effect/Stream"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"

export type Connection = "online" | "connecting" | "idle" | "offline" | "needsAuthentication" | "failed"

type Reported = Connection | "disconnected"

const offlineGrace = Duration.seconds(2)
export const connecting: Connection = "connecting"

const reportedOf = (status: AsyncResult.AsyncResult<ReplicaStatus.SpaceStatus, unknown>): Reported => {
  if (AsyncResult.isInitial(status)) return connecting
  if (AsyncResult.isFailure(status)) return "disconnected"
  return Match.value(status.value).pipe(
    Match.tagsExhaustive({
      Online: (): Reported => "online",
      SchemaUpdateAvailable: (): Reported => "online",
      NeedsAuthentication: (): Reported => "needsAuthentication",
      Failed: (): Reported => "failed",
      Offline: (): Reported => "disconnected",
      Connecting: (): Reported => connecting,
      Idle: (): Reported => "idle"
    })
  )
}

export const connectionChanges = <E, R,>(
  statuses: Stream.Stream<AsyncResult.AsyncResult<ReplicaStatus.SpaceStatus, unknown>, E, R>
): Stream.Stream<Connection, E, R> =>
  statuses.pipe(
    Stream.map(reportedOf),
    Stream.changes,
    Stream.switchMap((reported): Stream.Stream<Connection> => {
      if (reported !== "disconnected") return Stream.succeed(reported)
      return Stream.concat(
        Stream.succeed(connecting),
        Stream.fromEffect(Effect.sleep(offlineGrace)).pipe(Stream.as<Connection>("offline"))
      )
    })
  )
