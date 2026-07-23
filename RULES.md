# Repository Rules

Read this file before any work. Treat these rules as required for every package.

## Coordination

- Read `AGENTS.local.md` before editing.
- Only the main task owner edits or replaces `AGENTS.local.md`, and only when the repository level task changes to unrelated work.
- The main task owner records the task, branch, PR, ownership, checkpoints, commands, results, decisions, and next actions.
- The main task owner checks `git status` and the remote branch before mutation, rebases before every push, and pushes each independently verified fix.
- Never overwrite, revert, or reformat another contributor's work.
- Keep commits narrow.
- Use one production owner for overlapping files. Review agents must not edit production code, tests, shared ledgers, commits, branches, or PR state.

## Effect Source Of Truth

- Read the lockfile and each installed Effect ecosystem package's metadata, exports, declarations, and source before relying on its behavior.
- Treat installed package files as the source of truth for the installed version. Consult matching upstream tests only when they add needed behavioral or test harness evidence.
- Derive repositories and package directories from installed metadata. Do not hard code a user specific source checkout path.
- Prefer installed source and declarations, then matching upstream source and tests, over memory, tutorials, or prose documentation.
- Preserve typed failures, defects, interruption, scopes, and finalizers exactly as Effect defines them.

## Errors

- Give public domain failures a stable `_tag`.
- Use `Schema.TaggedErrorClass<Self>(identifier)("Tag", fields)` when an error must be decoded, encoded, or used in a Schema or RPC contract. Otherwise use `Data.TaggedError("Tag")<Fields>` or an existing Effect error type.
- Prefer `_tag` for public typed failure discrimination and serialized values. Use `instanceof` only when same realm class identity is an intentional internal contract.
- Use `Effect.catchTag` or `catchTags` for `_tag` failures. Use `catchReason` or `catchReasons` for nested tagged reasons. Use `catchIf`, `catchFilter`, or a specialized combinator for other selected typed failures.
- Use `Effect.catch` only when one handler intentionally covers the complete typed failure channel. It does not catch defects or interruption.
- Use `Effect.catchCause` only when the full `Cause` is required. Preserve or repropagate every cause case that is not intentionally recovered.
- Preserve interruption. Treat defects as defects by default. Translate a defect only at a documented integration boundary and retain the original cause as structured context.
- Preserve useful structured error context without including secrets or sensitive payloads.

## Services And Layers

- Model stable dependencies with `Context.Service`. Pass per call operation input as function arguments.
- Export explicit `Layer.Layer` values or constructors when the library owns a service implementation, acquisition, or release lifecycle.
- For a service that requires cleanup, acquire it with `Effect.acquireRelease` or another scoped `Effect`, and provide it with `Layer.effect`.
- Keep Layer construction configurable. Do not hide lifecycle, persistence, concurrency, or security choices behind defaults.
- Do not capture consumer specific runtime values in module global state. Pass them through service implementations, Layer constructors, or operation arguments.

## Composition And Effects

- Use `.pipe(...)` for readable linear data last composition. Use `Effect.gen` for sequential dependent workflows. Keep direct data first calls when they are clearer.
- Use `Effect.sync` only to suspend a synchronous side effect that is not expected to throw. Use `Effect.suspend` when the thunk returns an Effect.
- Use `Effect.log`, `logTrace`, `logDebug`, `logInfo`, `logWarning`, `logError`, or `logFatal`. Add structured fields with `Effect.annotateLogs`.
- Do not call global `console.log`, `console.warn`, or `console.error` in library code.
- Add `Effect.withSpan` or named `Effect.fn` spans at meaningful workflow, I/O, and remote boundaries. Add stable names and nonsecret attributes.
- Use Effect concurrency primitives with explicit owner, capacity, interruption, shutdown, and finalizer behavior.
- Prefer `Effect.forkChild`, `forkScoped`, or `forkIn`. Use `forkDetach` only when global lifetime, shutdown, and failure observation are explicit.
- Give queues, latches, subscriptions, and scopes a documented owner and cleanup path.

## Consumer API And Naming

- Optimize public APIs for low setup friction and explicit consumer control.
- For pipeable public combinators, use `Function.dual` when data first and data last forms materially improve use.
- Use `camelCase` for values and functions. Use `PascalCase` for types, services, schemas, and error classes.
- Add an `Error` suffix when it improves clarity. Preserve established class names and serialized `_tag` values for public or protocol errors.
- Name Layer values and constructors so their implementation, configuration, or lifecycle is clear.
- Keep internal modules under `src/internal`. Export only deliberate consumer APIs from package entry points.
- Avoid redundant wrappers, aliases, and abstractions when an Effect primitive already expresses the contract.
- Reuse established helpers and schemas before introducing another representation of the same concept.

## Tests And Changesets

- Use Red, Green, Refactor for every bug fix.
- A bug is not confirmed until an observable regression test fails for the expected reason before the production change and passes afterward.
- Exercise production composition. Replace only true external boundaries.
- Write Effect tests with `@effect/vitest` `it.effect` or `effect`.
- Use `TestClock` from `effect/testing` for virtual time. Coordinate concurrency with production `Deferred`, `Latch`, `Queue`, fiber, and scope APIs. Do not use sleeps as synchronization.
- Do not add tests that merely mirror private control flow.
- Keep one initial release changeset for all packages changed before the first release.
