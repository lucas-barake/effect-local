# @lucas-barake/effect-local-test

Production shaped SQLite test layers, peer transports, and deterministic fault injection for Effect Local.

`TestPeer` exercises both durable packets and current connection transient delivery. Its transient route is bounded,
nonblocking, and no replay so tests can prove offline loss and slow subscriber dropping without durable custody.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme) for testing recipes, convergence examples, and API reference.
