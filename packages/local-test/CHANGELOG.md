# @lucas-barake/effect-local-test

## 0.2.0

### Minor Changes

- [#1](https://github.com/lucas-barake/effect-local/pull/1) [`4b0d162`](https://github.com/lucas-barake/effect-local/commit/4b0d162d2914d72edf6be945110fa4a475707432) Thanks [@lucas-barake](https://github.com/lucas-barake)! - Initial release of Effect Local and its core, SQL, RPC, browser, and testing packages.

  Local first document synchronization built on Automerge, with SQLite backed replicas on Node and in the browser, and
  an authenticated store and forward peer relay over Effect RPC. The relay is a cluster of per device entities, so more
  than one relay node can serve one database, and durable custody is injectable behind `RelayInboxStore` with a
  `SqlClient` implementation for SQLite, PostgreSQL, and MySQL.

### Patch Changes

- Updated dependencies [[`4b0d162`](https://github.com/lucas-barake/effect-local/commit/4b0d162d2914d72edf6be945110fa4a475707432)]:
  - @lucas-barake/effect-local@0.2.0
  - @lucas-barake/effect-local-sql@0.2.0
