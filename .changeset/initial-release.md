---
"@lucas-barake/effect-local": minor
"@lucas-barake/effect-local-sql": minor
"@lucas-barake/effect-local-rpc": minor
"@lucas-barake/effect-local-browser": minor
"@lucas-barake/effect-local-test": minor
"@lucas-barake/effect-local-expo": minor
---

Initial release of Effect Local and its core, SQL, RPC, browser, Expo, and testing packages.

Local first state uses optimistic mutations in local SQLite and an authenticated server reconciled mutation log. The
server assigns a dense total order to accepted mutations, exact retries return durable receipts, clients replay
pending work after catch up, and Effect Atom exposes reactive entities, queries, receipts, and status. Optional field
semantics cover counters and sets without making CRDT metadata part of ordinary state.

Authoritative history and terminal receipts have explicit retained targets and hard admission caps. Maintenance
publishes verified immutable state snapshots before reclaiming either prefix. Fresh or lagged clients durably stage
bounded snapshot pages, atomically install canonical state, and continue from the snapshot sequence. Expired receipt
watermarks preserve at most once execution after private results are reclaimed. Client receipts and accepted evidence
are bounded without deleting rows still required by pending mutations.

Effect Cluster runs both sides. `SyncServer.layer` builds the whole server from a definition and four authorization
callbacks: the authenticated gateway, one entity per space, the durable store, the ephemeral hub, HMAC-signed principal
assertions, write-triggered compaction, and a maintenance singleton. It runs on one runner or many. Several runners must
share one `assertionSecret`, because each process otherwise generates its own random secret and rejects the assertions
another runner signed.
In the browser, `BrowserReplica.layer` turns every tab of an origin into a runner of one cluster, hosts the replica on
the leader tab that owns SQLite, fails over when that tab closes, and `ReplicaAtom.make` exposes it as an Atom graph.
Tabs from different deploys never share a cluster. The build with the higher definition version takes the database
over. Between builds of the same version the build of the tab that started last wins. Every tab of the losing build
fails with a typed `BuildSuperseded` error that the app can turn into a reload prompt.
On React Native, `ExpoReplica.layer` runs the same replica over `expo-sqlite` and `expo-crypto`, in Expo Go or a native
build, and `ReactNativeSocket.layerWebSocketConstructor` supplies React Native's WebSocket to the sync client.
Server storage migrates its own schema by default. Where the database role may not run DDL,
`Migrations.renderServer` reads the database and returns the exact SQL a DBA applies, and
`migration: { mode: "verify" }` refuses to serve with `StorageMigrationPending` until the schema is current.
Every limit has a documented default, so a working client or server needs a few lines, and every limit can be
overridden.
