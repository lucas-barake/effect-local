---
"@lucas-barake/effect-local": minor
"@lucas-barake/effect-local-sql": minor
"@lucas-barake/effect-local-rpc": minor
"@lucas-barake/effect-local-browser": minor
"@lucas-barake/effect-local-test": minor
---

Initial release of Effect Local and its core, SQL, RPC, browser, and testing packages.

Local first document synchronization built on Automerge, with SQLite backed replicas on Node and in the browser, and
an authenticated store and forward peer relay over Effect RPC. The relay is a cluster of per device entities, so more
than one relay node can serve one database, and durable custody is injectable behind `RelayInboxStore` with a
`SqlClient` implementation for SQLite, PostgreSQL, and MySQL. Interactive replica operations receive priority over
queued inbound synchronization at the shared SQL connection. Interactive work retains its existing concurrency, and a
waiting background operation closes the current interactive batch so synchronization still makes progress.
