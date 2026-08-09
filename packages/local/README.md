# @lucas-barake/effect-local

Local first domain primitives for Effect v4.

`PeerTransport.Connection` carries tagged durable and transient inbound values. Durable sends have settlement and
replay semantics supplied by the adapter. Transient sends are document scoped, best effort, and have no settlement or
replay contract.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme) for installation, domain modeling, replica operations, sync, backup, and API reference.
