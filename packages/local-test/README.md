# @lucas-barake/effect-local-test

Production shaped test Layers for Effect Local.

`TestServer.layer` adapts the real `ServerStore` to `SyncEngine`. `TestReplica.layer` uses the real `SqlReplica`
composition. `FaultInjection.layer` can partition and heal the link, drop the next receipt after authoritative commit,
and duplicate the next pull page so applications can test offline writes, ambiguous acknowledgements, reconnect, and
deduplication without replacing the persistence engine.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
