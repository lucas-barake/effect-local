# Repository Rules

Read this file before any work. Treat these rules as required for every package.

## Coordination

- Check `git status` and the remote branch before mutation, rebase before every push, and push each independently verified fix.
- Never overwrite, revert, or reformat another contributor's work.
- Keep commits narrow.
- Use one production owner for overlapping files. Review agents must not edit production code, tests, commits, branches, or PR state.

## Effect Source Of Truth

- Read the lockfile and each installed Effect ecosystem package's metadata, exports, declarations, and source before relying on its behavior.
- Treat installed package files as the source of truth for the installed version. Consult matching upstream tests only when they add needed behavioral or test harness evidence.
- Derive repositories and package directories from installed metadata. Do not hard code a user specific source checkout path.
- Prefer installed source and declarations, then matching upstream source and tests, over memory, tutorials, or prose documentation.
- Preserve typed failures, defects, interruption, scopes, and finalizers exactly as Effect defines them.

## Errors

- Define every library owned typed error with `Schema.TaggedErrorClass<Self>(identifier)("Tag", fields)`.
- Give every typed failure a stable `_tag`. Do not add untagged or `Data.TaggedError` library error types.
- Discriminate tagged errors with `_tag`. Do not use `instanceof` for error discrimination.
- Do not define shared error transformers such as `toSomeError`, `mapErrors`, or helpers that accept `unknown` and return another error. Handle each inferred failure directly at the call site with explicit `_tag` based `Effect.catchTag`, `catchTags`, `catchReason`, or `catchReasons` branches. Use `catchCause` at the call site only when the full Cause must be preserved. Accept small duplication so error channels stay narrow and every handled failure remains explicit.
- The duplication the rule above accepts is inline construction at the call site, not a helper that wraps it. Do not define an error constructor inside the module that raises the error. Construct the error inline, however many times the module needs it. When more than one module raises the same error, put exactly one constructor in a module under `src/internal` and import it. A module local helper reads as private shorthand, so the next module that needs the same error copies the helper instead of importing it, and one condition ends up with several spellings that no reader can tell apart without diffing them.
- Do not use generic `E` intersections, structural `_tag` probes, `typeof error === "object"`, or property existence checks to claim that an Effect failure is a specific tagged error. Let the Effect error channel prove the tag and use the matching typed catch combinator. If the combinator does not typecheck, the call site has not proved that it can fail with that error.
- Never pair `Effect.catchIf`, `catchFilter`, or another predicate catch with a broad `isXError` guard for a tagged failure. A runtime guard can accept values that the call site's typed failure channel did not establish. Catch a tagged wrapper with `Effect.catchTag` or `catchTags`, and catch a nested tagged reason with `catchReason` or `catchReasons`. Use guards only at genuinely untyped boundaries such as an `unknown` value or a preserved `Cause`, where tag combinators cannot prove the type.
- Use `Effect.catchIf`, `catchFilter`, or a specialized predicate combinator only for conditions that are not represented by a stable tag.
- Use `Effect.catch` only when one handler intentionally covers the complete typed failure channel. It does not catch defects or interruption.
- Do not use official `Effect.orDie` in any call or reference form. Do not reproduce it with official `Effect.catch` when the handler only passes the complete caught error to official `Effect.die`. When a typed failure is intentionally unrecoverable, select it at the call site with `Effect.catchTag`, `catchTags`, `catchReason`, or `catchReasons`, and call `Effect.die(error)` in every selected branch. Leave every unselected tag or reason in the typed error channel. Do not hide the branches in a shared helper.
- Use `Effect.catchCause` only when the full `Cause` is required. Preserve or repropagate every cause case that is not intentionally recovered.
- Preserve interruption. Treat defects as defects by default. Translate a defect only at a documented integration boundary and retain the original cause as structured context.
- Preserve useful structured error context without including secrets or sensitive payloads.

## Services And Layers

- Model stable dependencies with `Context.Service`. Pass per call operation input as function arguments.
- Export explicit `Layer.Layer` values or constructors when the library owns a service implementation, acquisition, or release lifecycle.
- For a service that requires cleanup, acquire it with `Effect.acquireRelease` or another scoped `Effect`, and provide it with `Layer.effect`.
- Keep Layer construction configurable. Do not hide lifecycle, persistence, concurrency, or security choices behind defaults.
- Do not capture consumer specific runtime values in module global state. Pass them through service implementations, Layer constructors, or operation arguments.
- Resolve dependencies once while the Layer is being built and close over them. A service method must not leave a service in its own requirement channel.
- A service's methods should be `Effect<A, E, never>`. Leak a requirement only when the dependency is genuinely per call, such as a request scoped context or an ambient `Scope` the caller owns, and say why at the definition.
- A leaked requirement is a real cost: it spreads to every transitive caller, it lets a method be invoked under a context the Layer never validated, and it hides which services an implementation actually needs behind the call sites instead of stating it once at construction.
- A Layer must capture every service used later by its methods.
- Export a constructor or use `Layer.fresh` when a context sensitive Layer must build independently more than once under one memo map.
- Wire reactive consumers through the shared `Atom.runtime` factory so every atom runtime builds under one `Layer.MemoMap` and shares service instances app wide. Do not pair a bespoke `ManagedRuntime` with `Atom.context({ memoMap })` to re-expose its context to atoms; reserve `ManagedRuntime` for non reactive entry points that need explicit disposal, such as worker bootstraps.

## Composition And Effects

- Use `.pipe(...)` for readable linear data last composition. Use `Effect.gen` for sequential dependent workflows. Keep direct data first calls when they are clearer.
- Use `Effect.sync` only to suspend a synchronous side effect that is not expected to throw. Use `Effect.suspend` when the thunk returns an Effect.
- Inside `Effect.gen`, execute synchronous code directly. Never write `yield* Effect.sync(...)`; the generator body is already suspended by the enclosing Effect.
- Use `Effect.log`, `logTrace`, `logDebug`, `logInfo`, `logWarning`, `logError`, or `logFatal`. Add structured fields with `Effect.annotateLogs`.
- Do not call global `console.log`, `console.warn`, or `console.error` in library code.
- Add `Effect.withSpan` or named `Effect.fn` spans at meaningful workflow, I/O, and remote boundaries. Add stable names and nonsecret attributes.
- Use Effect concurrency primitives with explicit owner, capacity, interruption, shutdown, and finalizer behavior.
- Prefer `Effect.forkChild`, `forkScoped`, or `forkIn`. Use `forkDetach` only when global lifetime, shutdown, and failure observation are explicit.
- Keep execution inside Effect. Do not call `Effect.runFork`, `Effect.runPromise`, `Effect.runSync`, or another detached `run*` runner from inside Effect code. Bridge external events in with `Deferred`, `Queue`, `FiberSet`, or `Effect.runtime`, so execution stays under the current scope, context, and interruption rules. Reserve `run*` calls for genuine entry points such as process mains and worker bootstraps.
- Prefer Effect returning combinators over throwing synchronous variants inside Effect code, for example `Schema.decodeUnknownEffect` over `Schema.decodeUnknownSync`. Contain unavoidable throwing variants at the non Effect boundary that requires them.
- Give queues, latches, subscriptions, and scopes a documented owner and cleanup path.
- Attach cleanup immediately after acquiring a native resource so every later failure and interruption path releases it.
- Never wrap `Effect.sync` in `Semaphore.withPermit`. Synchronous JavaScript cannot interleave. Use a semaphore only when one invariant spans an effectful suspension.
- Recheck mutable admission, quota, and idempotency state inside the same lock or transaction that performs the write.

## Persistence And Validation

- Never trust an external boundary. Decode every value that crosses into the program — SQL rows, wire payloads, archives, file contents — with a Schema. For SQL reads use `SqlSchema` request and result decoding; never assert row shapes with a bare `sql<T>` type parameter in library code.
- Decode external, archive, and durable values with their domain Schemas before calling branded constructors.
- Validate redundant persisted metadata, types, hashes, and sequence fields before replay.
- Encode composite durable keys with an unambiguous structured representation.
- Enforce uniqueness under every key domain used by downstream state and routing.
- Deduplicate reactivity and subscription keys before registration.
- Validate migration descriptors against the installed Migrator scheduling rules.

## Consumer API And Naming

- Optimize public APIs for low setup friction and explicit consumer control.
- For pipeable public combinators, use `Function.dual` when data first and data last forms materially improve use.
- Express a configurable duration as `Duration.Input`, never as a bare number named with a unit suffix such as `Millis`. `Duration.Input` lets a consumer write `"30 seconds"`, `Duration.minutes(5)`, or a raw number, and it states the unit in the type instead of in the identifier.
- Convert a configured duration once, with `Duration.toMillis`, at the point the Layer or service is constructed, and close over the result. Do not convert per call and do not thread `Duration.Input` into arithmetic.
- A duration that crosses a wire protocol or is persisted stays a plain number with its unit in the name. `Duration` has no stable serialized encoding, so a protocol or column must name its unit.
- Name every stored `Layer.Layer` value `layer` or `layerX`, where `X` is a PascalCase descriptive suffix. A suffix form such as `testLayer` is invalid. Use `PascalCase` for types, services, schemas, and error classes. Use `camelCase` for other values and for functions, including functions that return a Layer.
- Add an `Error` suffix when it improves clarity. Preserve established class names and serialized `_tag` values for public or protocol errors.
- Name Layer values and constructors so their implementation, configuration, or lifecycle is clear.
- Keep internal modules under `src/internal`. Export only deliberate consumer APIs from package entry points.
- Avoid redundant wrappers, aliases, and abstractions when an Effect primitive already expresses the contract.
- Do not export aliases of Schema codec constructors such as `export const decodeFoo = Schema.decodeUnknownSync(Foo)` or `export const encodeFoo = Schema.encodeSync(Foo)`. Call the codec directly at each use site so the schema and the operation stay visible at the boundary.
- Do not hand write a constructor function for a schema that exists as a value. Every schema carries `.make`, which applies constructor defaults and type side checks, and `Schema.tag` makes `_tag` optional in construction, so `Foo.make({ id })` supersedes `export const foo = (id) => ({ _tag: "Foo", id })`. A hand written constructor skips the schema's own checks, states the tag a second time where it can drift from the schema, and hides which schema the value belongs to. Use `makeOption` or `makeEffect` when a construction failure must stay a value rather than a thrown defect.
- The exception is a type whose schema is built per call by a generic function, such as `CommandOutcome`, where the exported types are interfaces and no schema value exists to construct from. There a plain constructor is the only option.
- Reuse established helpers and schemas before introducing another representation of the same concept.

## Tests And Changesets

- Use Red, Green, Refactor for every bug fix.
- A bug is not confirmed until an observable regression test fails for the expected reason before the production change and passes afterward.
- Exercise production composition. Replace only true external boundaries.
- Write Effect tests with `@effect/vitest` `it.effect` or `effect`.
- Use `TestClock` from `effect/testing` for virtual time. Coordinate concurrency with production `Deferred`, `Latch`, `Queue`, fiber, and scope APIs. Do not use sleeps as synchronization.
- Never synchronize a test on wall clock time. No `it.live` with `Effect.sleep`, real timer polling, or repetition loops that only pass when the machine is fast enough. A test whose runtime scales with real I/O against a fixed `testTimeout` is non deterministic by construction.
- Every wait must rendezvous on observable state: a `Deferred`, `Latch`, `Queue`, or fiber join. Run time based code paths under `it.effect` so sleeps, timeouts, and retries resolve through `TestClock`, and advance them with `TestClock.adjust` or `setTime`.
- When a hazard lives below the fiber, such as a cancel-less syscall that can outlive the interrupted fiber that issued it, observe it with a probe hooked into the callback boundary and gate it into the exact interleaving under test. An abandoned fiber runs nothing else, so Effect level tracking such as `Ref` or `ensuring` never sees it, and no real time delay can settle it deterministically.
- Do not add tests that merely mirror private control flow.
- Keep one initial release changeset for all packages changed before the first release.
