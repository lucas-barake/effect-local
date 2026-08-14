# @lucas-barake/effect-local-rpc

Authenticated Effect RPC synchronization for Effect Local.

One `SyncRpc.Rpcs` group carries mutation submission, ordered pulls, snapshot bootstrap pages, wake streams, presence
publication, and presence watch streams over one WebSocket. `SyncServer.layer` keeps that public authenticated facade
and routes each operation through `SpaceEntity.Client`. `SyncClient.layer` adapts the generated typed client to
`SyncEngine`.

`SpaceEntity` routes one space through five volatile Cluster entity types. `SpaceAdmissionEntity` serializes Submit and
Discard. `SpaceReadEntity` forks Pull and immutable Bootstrap page reads. `SpaceWatchEntity` forks long lived sync and
presence streams. `SpacePresencePublishEntity` bounds concurrent presence publications. Separate finite mailboxes keep
paused Bootstrap pages and long lived streams out of mutation admission. `SpaceAttachmentControlEntity` bounds
prepare, finalize, and download-grant work. `SpaceEntity.layerClient` aggregates the five
generated clients for the public facade. `SpaceEntity.layer(options)` composes handlers and that client.

```ts
import * as PresenceHub from "@lucas-barake/effect-local-rpc/PresenceHub"
import * as SpaceEntity from "@lucas-barake/effect-local-rpc/SpaceEntity"
import * as Layer from "effect/Layer"

const layerPresence = PresenceHub.layer({
  capacity: 1_024,
  maximumWatchersPerSpace: 1_024,
  authorize: authorizePresence
})

const layerSpace = SpaceEntity.layer({
  admissionMailboxCapacity: 64,
  readMailboxCapacity: 64,
  watchMailboxCapacity: 2_048,
  presencePublicationMailboxCapacity: 256,
  attachmentControlMailboxCapacity: 256,
  maximumConcurrentBootstrapAuthorizations: 64,
  maximumConcurrentBootstrapPagesPerSpace: 8,
  maximumConcurrentPresencePublicationsPerSpace: 64,
  maximumConcurrentAttachmentControlsPerSpace: 16
}).pipe(
  Layer.provide(layerStore),
  Layer.provide(layerPresence),
  Layer.provide(layerRunner)
)
```

All nine numeric `SpaceEntity` options are required positive safe integers. A full mailbox maps to `ServerUnavailable`.
Bootstrap concurrency is fail fast. `maximumConcurrentBootstrapAuthorizations` bounds assertion verification and
bootstrap preparation across the Layer. `maximumConcurrentBootstrapPagesPerSpace` bounds published page reads per
space. Saturation reports typed `CapacityExceeded` with resource `bootstrap authorizations` or `bootstrap pages`.

Attachment control uses the same authenticated `ProtocolSession`. `AttachmentRpcClient.layerFromSession` requests
grants through RPC, while `AttachmentDirectHttpClient.layer` sends bytes only to separately allowlisted provider upload
and download origins. The application server never exposes an attachment byte route. The application supplied
`AttachmentObjectStore` adapter owns provider signing, verified finalization, and deletion.

For one process, provide `SingleRunner.layer` with the selected runner storage. A Node multi runner deployment provides
`SocketRunner.layer` with `NodeClusterSocket.layerSocketServer` and `NodeClusterSocket.layerClientProtocol`, then supplies
its chosen `RunnerStorage`, `MessageStorage`, `RunnerHealth`, `RpcSerialization`, and `ShardingConfig` Layers. The package
does not select those deployment policies.

The client pending row retains custody until Submit returns. `ServerStore` then retains the authoritative application
event and receipt. A runner failure before admission makes the volatile entity call fail, so reconciliation resubmits the
same identity. A lost reply after SQL commit is recovered from the stored receipt. Cluster mailbox persistence is not
used because Effect `4.0.0-beta.103` retains completed payloads and replies without per-request retention control. A custom
`ServerStore` service can route by `spaceId` to database shards. The provided SQL layer uses the supplied `SqlClient` as
one partition. Pulls return either a bounded dense suffix or `BootstrapRequired`. Bootstrap pages repeat an immutable
manifest and stay under the configured frame bound. Wake and presence streams are live hints routed through the
current owner of the space.

## Authentication lifecycle

Authentication follows Effect RPC middleware conventions. The client calls `CredentialProvider.acquire` for every
RPC, writes the redacted bearer credential to the request, and includes its nonnegative generation. The server uses
`Authenticator` to provide a JSON `Principal`. `SyncServer.layerHandlers` requires a `PrincipalAssertion.Issuer`, while
`SpaceEntity.layerHandlers` requires the matching `PrincipalAssertion.Verifier`. The assertion is opaque on the
internal Cluster hop. The entity verifies it before deriving the principal, so browser payloads never carry or choose
principal authority. Applications own assertion authenticity, expiry, and key rotation. Do not encode unsigned
principal JSON as a production assertion.

`ServerStore.layer` requires explicit access, mutation admission, and read callbacks. `PresenceHub.layer` requires a
tagged publish or watch callback and includes the claimed client ID for publish policy. Use the named `layerTrusted`
constructors only where allow all is intentional.

A provider backed by a `SubscriptionRef` can rotate credentials without rebuilding the replica or socket:

```ts
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"

const makeAuthentication = (initialBearer: Redacted.Redacted) =>
  Effect.gen(function*() {
    const credentials = yield* SubscriptionRef.make<Authentication.Credential>({
      generation: 0,
      bearer: initialBearer
    })
    const provider = Authentication.CredentialProvider.of({
      acquire: SubscriptionRef.get(credentials),
      awaitChange: (rejectedGeneration) =>
        SubscriptionRef.changes(credentials).pipe(
          Stream.filter((credential) => credential.generation !== rejectedGeneration),
          Stream.runHead,
          Effect.flatMap(Option.match({
            onNone: () => Effect.never,
            onSome: Effect.succeed
          }))
        )
    })
    return {
      layer: Authentication.layerClient.pipe(
        Layer.provide(Layer.succeed(Authentication.CredentialProvider, provider))
      ),
      rotate: (bearer: Redacted.Redacted) =>
        SubscriptionRef.update(credentials, (current) => ({
          generation: current.generation + 1,
          bearer
        }))
    }
  })
```

`CredentialRejected` means the bearer is invalid, revoked, or expired. Reconciliation reports
`NeedsAuthentication`, stops retrying that generation, and calls `awaitChange`. Once the provider exposes a different
generation, synchronization resumes through the same replica and WebSocket. `AuthenticatorUnavailable` instead means
the verifier or a dependency is unavailable. It reports `Offline` and retries with capped exponential backoff.
`AuthorizationDenied` means the authenticated principal lacks permission. It reports `Failed` and is not retried.

## Timeouts and retry policy

`SyncClient.layerWithOptions` and `PresenceClient.layerWithOptions` accept `sessionAcquisitionTimeout` and
`rpcTimeout` as `Duration.Input`. Both default to 10 seconds. The session timeout bounds negotiation and
renegotiation. The RPC timeout bounds each unary RPC and stream acquisition. Expiry interrupts the operation and
returns typed `OperationTimeout`. An established watch may remain idle. Socket ping and reconnect behavior detect a
dead connection without turning healthy inactivity into retry traffic. Reconciliation reports `Offline` and retries
timeouts with the same capped exponential policy as `ServerUnavailable` and `AuthenticatorUnavailable`.

Configure reconciliation with `retryDelay` and `maximumRetryDelay` on `SqlReplica`. Delays start at 1 second by
default, double after each transient failure, and stop growing at the default 1 minute cap. A successful pass resets
the attempt count. `maximumRetryDelay` must be greater than or equal to `retryDelay`.

`SyncClient.layerProtocolSocket` exposes Effect's socket reconnect options:

```ts
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as Schedule from "effect/Schedule"

const layerRpcProtocol = SyncClient.layerProtocolSocket({
  retryTransientErrors: true,
  retryPolicy: Schedule.exponential("250 millis").pipe(
    Schedule.jittered,
    Schedule.upTo({ times: 8 })
  )
})
```

The socket reconnect policy is separate from reconciliation backoff. `retryTransientErrors: true` keeps socket open
errors internal while retries remain. If a finite `retryPolicy` exhausts, that protocol instance stops opening the
socket. Outstanding operations still fail at their configured timeout. Rebuilding the protocol Layer starts a new
socket acquisition lifecycle.

`PresenceHub.maximumWatchersPerSpace` caps active presence streams. It is independent from `capacity`, the sliding
per space update queue depth. Excess presence streams fail with `CapacityExceeded { resource: "presence watchers",
limit }` and release their allowance on interruption or authorization failure. The live population is exported as
`effect_local_server_presence_watcher_count`.

`ServerStore.maximumWatchersPerSpace` separately caps sync streams. Accepted admissions publish a shared wake after
commit. Delivering it performs no SQLite transaction or space row write per watcher. The benchmark at
`bench/Fanout.bench.ts` exercises the production composed path with 64, 256, and 1,024 watchers.

Use `SyncRpc.layerJson` for the WebSocket serialization Layer. It bounds each complete UTF-8 JSON frame and makes
remote defects opaque. The high level Effect Node server does not expose the underlying `ws` `maxPayload` option, so
production ingress must also enforce `SyncRpc.maximumFrameBytes` with a reverse proxy or a lower level upgrade handler.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
