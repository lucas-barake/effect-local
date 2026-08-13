# @lucas-barake/effect-local-test

Production shaped test Layers for Effect Local.

`TestServer.layer` adapts the real `ServerStore` to `SyncEngine`. `TestReplica.layer` uses the real `SqlReplica`
composition. `FaultInjection.layer` addresses every fault by `SpaceId`. It can partition and heal one link, drop that
space's next receipt after authoritative commit, and duplicate that space's next pull page. Tests can prove that one
partitioned space does not delay another while retaining the real persistence and reconciliation composition.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
