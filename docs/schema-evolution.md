# Schema Evolution

## Stable identities

Document names, mutation names, projection names, query names, and their numeric versions are protocol identities.
Renaming one changes the protocol. `ReplicaDefinition.hash` captures the encoded schema surface and is checked during
browser session startup and restore.

## Opening an evolved definition

Bootstrap no longer requires an exact definition hash match. When the stored hash differs from the current definition,
the replica opens if and only if every stored document row can reach the current build:

- Every stored document type still exists in the definition.
- Every stored `schema_version` is at most the current document version.
- Every stored version below the current version reaches it through the registered migration chain.

When the check passes, the stored definition hash is updated in the same bootstrap transaction and a startup workflow
(`ReplicaEvolution`) migrates stale documents and rebuilds changed projections before the replica serves requests.
When it fails — a removed document type with rows, or a version bump without a covering chain — bootstrap fails with
`ProtocolMismatch` and metadata is untouched, so the previous build still opens. Because the compatibility check runs
against stored data, a rolled back deployment reopens the replica under the previous definition as long as its own
documents remain decodable.

Peer sync and browser session gates still require identical definition hashes: two peers on different definitions
never exchange changes.

## Documents

Increment the document version when encoded meaning changes, and register a `Document.migration` for every prior
version that may still be stored. Each step declares the source version, the schema that decodes stored values at that
version, and a pure `migrate` function returning the next version's value. `Document.make` rejects duplicate or gapped
chains eagerly, and every step's output is validated against the next schema at decode time. A field may be added
without a version bump only when older stored values decode through an explicit schema default; otherwise bump the
version and migrate.

Migrations are local decode capability, not protocol surface: they are excluded from `ReplicaDefinition.hash`, so
shipping an additional migration does not change the protocol.

Recovery decodes every stored value with the decoder matching its stored version and applies the chain, so snapshots
always carry the current type. At startup, `ReplicaEvolution` materializes each stale document by appending a normal
Automerge change that rewrites the value to the current encoding and bumps the stored version. Automerge history is
never rewritten — a destructive rewrite would lose causal meaning and break later convergence with an offline peer. A
stored version with no registered chain fails with `UnsupportedDocumentVersion`; a document whose history cannot be
recovered is quarantined and skipped, never silently reinterpreted.

## Mutations and receipts

Mutation payload, success, and domain error schemas are part of durable receipts and Cluster replies. Keep a mutation
version decodable for as long as its receipt may be retried. A new behavior with incompatible input or output should
use a new version or protocol name.

## Projections

Projection tables are derived state. When a projection's version, row schema, or table changes, the registry entry is
reconciled at startup: stale rows are cleared, the entry is marked `Rebuilding`, and `ReplicaEvolution` re-projects
every document of that type from verified canonical snapshots before marking the projection `Ready`. A projection
added over existing documents rebuilds the same way. Queries that depend on a projection are blocked until it is
`Ready`, so readers never observe a partially rebuilt table.

Query results are schema decoded. A query deployment must accept the active projection version and return its declared
success schema. Projection and query rollout should therefore happen in one application shell deployment.

## Restore

Canonical restore checks the expected definition hash before installation. A portable document import checks document
name and schema version, decodes the value through the current schema, and writes a new Automerge history. This is the
preferred boundary for long lived user readable exports.

The strict hash check means an old backup can become incompatible after definition evolution. Applications must retain
a matching build for restore of old backups; portable document import remains the version tolerant boundary.
