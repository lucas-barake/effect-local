# Schema Evolution

## Stable identities

Document names, mutation names, projection names, query names, and their numeric versions are protocol identities.
Renaming one changes the protocol. `ReplicaDefinition.hash` captures the encoded schema surface and is checked during
browser session startup and restore.

## Documents

The current beta pins each durable replica to its exact `ReplicaDefinition.hash`. It does not yet open an existing
replica under a changed document definition, including an otherwise additive field. Additive schema design remains
the compatibility direction, not a shipped in place migration capability.

When versioned migration support is added, a field may be added only when older stored values decode through an
explicit schema default or transformation. Increment the document version when encoded meaning changes.

Do not silently reinterpret an existing encoded value. A future compatible migration facility must define a new
version, retain versioned decoders, and run an explicit migration workflow until every reachable canonical document
can be migrated or intentionally quarantined. The current beta does not provide that decoder registry.

Automerge changes are never rewritten as a convenience migration. A destructive rewrite loses causal meaning and can
break later convergence with an offline peer.

## Mutations and receipts

Mutation payload, success, and domain error schemas are part of durable receipts and Cluster replies. Keep a mutation
version decodable for as long as its receipt may be retried. A new behavior with incompatible input or output should
use a new version or protocol name.

## Projections

Projection tables are derived state. Increment the projection version, create a shadow physical table, rebuild from
verified canonical snapshots, then publish the new binding atomically. Readers keep using the previous table until
publish succeeds.

Query results are schema decoded. A query deployment must accept the active projection version and return its declared
success schema. Projection and query rollout should therefore happen in one application shell deployment.

## Restore

Canonical restore checks the expected definition hash before installation. A portable document import checks document
name and schema version, decodes the value through the current schema, and writes a new Automerge history. This is the
preferred boundary for long lived user readable exports.

The strict hash and version checks mean an old backup can become incompatible after definition evolution. Applications
must retain a matching build for restore until compatible manifests and versioned decoder migration are implemented.
