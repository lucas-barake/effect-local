# @lucas-barake/effect-local-sql

SQLite persistence and authoritative mutation ordering for Effect Local.

`SqlReplica.layer` provides the public replica from a durable local store, query executor, and scoped reconciler.
Local SQLite stores canonical entities, visible entities, pending mutations, terminal receipts, accepted entries, and
the server cursor. Optimistic writes and reconciliation are transactional.

`ServerStore.layer` authenticates through application supplied callbacks, deduplicates stable mutation identities,
stores terminal rejections, assigns the next dense sequence to accepted mutations, and materializes authoritative
state in the same SQL transaction. `SyncEngine` is the transport neutral boundary used by direct tests and the RPC
client.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme) and
[durability notes](https://github.com/lucas-barake/effect-local/blob/main/docs/durability.md).
