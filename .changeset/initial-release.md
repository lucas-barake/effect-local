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
