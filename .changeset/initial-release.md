---
"@lucas-barake/effect-local": minor
"@lucas-barake/effect-local-sql": minor
"@lucas-barake/effect-local-rpc": minor
"@lucas-barake/effect-local-browser": minor
"@lucas-barake/effect-local-test": minor
---

Initial release of Effect Local and its core, SQL, RPC, browser, and testing packages.

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
callbacks: the authenticated gateway, one entity per space, the durable store, the ephemeral hub, HMAC-signed
principal assertions, write-triggered compaction, and a maintenance singleton. It runs unchanged on one runner or many.
In the browser, `BrowserReplica.layer` turns every tab of an origin into a runner of one cluster, hosts the replica on
the leader tab that owns SQLite, fails over when that tab closes, and `ReplicaAtom.make` exposes it as an Atom graph.
Every limit has a documented default, so a working client or server needs a few lines, and every limit can be
overridden.
