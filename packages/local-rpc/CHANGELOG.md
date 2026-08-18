# @lucas-barake/effect-local-rpc

## 0.1.0

### Minor Changes

- [#1](https://github.com/lucas-barake/effect-local/pull/1) [`4b0d162`](https://github.com/lucas-barake/effect-local/commit/4b0d162d2914d72edf6be945110fa4a475707432) Thanks [@lucas-barake](https://github.com/lucas-barake)! - Initial release of Effect Local and its core, SQL, RPC, browser, and testing packages.

  Local first state uses optimistic mutations in local SQLite and an authenticated server reconciled mutation log. The
  server assigns a dense total order to accepted mutations, exact retries return durable receipts, clients replay
  pending work after catch up, and Effect Atom exposes reactive entities, queries, receipts, and status. Optional field
  semantics cover counters and sets without making CRDT metadata part of ordinary state.

  Authoritative history and terminal receipts have explicit retained targets and hard admission caps. Maintenance
  publishes verified immutable state snapshots before reclaiming either prefix. Fresh or lagged clients durably stage
  bounded snapshot pages, atomically install canonical state, and continue from the snapshot sequence. Expired receipt
  watermarks preserve at most once execution after private results are reclaimed. Client receipts and accepted evidence
  are bounded without deleting rows still required by pending mutations.

### Patch Changes

- Updated dependencies [[`4b0d162`](https://github.com/lucas-barake/effect-local/commit/4b0d162d2914d72edf6be945110fa4a475707432)]:
  - @lucas-barake/effect-local@0.2.0
  - @lucas-barake/effect-local-sql@0.2.0
