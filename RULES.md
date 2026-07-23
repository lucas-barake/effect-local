# Repository Rules

Read this file before any work. Treat these rules as required for every package.

## Coordination

- Read `AGENTS.local.md` before editing.
- For a new task, replace the old ledger with a detailed ledger for the new task.
- Record the task, branch, PR, ownership, checkpoints, commands, results, decisions, and next actions.
- Check `git status` and the remote branch before editing or pushing.
- Rebase on the remote task branch before every push.
- Never overwrite, revert, or reformat another contributor's work.
- Keep commits narrow. Push each independently verified fix.
- Use one production owner for overlapping files. Review agents remain read only.

## Effect Source Of Truth

- Read installed package metadata and the lockfile before relying on Effect behavior.
- Verify APIs and conventions against the matching official Effect source and tests under `~/src/oss/.versions/effect/<version>/`.
- Prefer official source and tests over memory, examples, or prose documentation.
- Preserve typed failures, defects, interruption, scopes, and finalizers exactly as Effect defines them.

## Errors

- Model every library error as a `Schema.TaggedError` class with a stable `_tag`.
- Discriminate errors by `_tag`. Do not use `instanceof` for Effect errors.
- Prefer `catchTag` or `catchTags` when handling known typed failures.
- Treat broad `catch`, `catchCause`, and equivalent handlers as exceptional. Use them only when every failure or cause case is intentionally handled and defects and interruption stay correct.
- Do not convert defects or interruption into ordinary failures.
- Preserve useful structured context in errors without leaking secrets.

## Services And Layers

- Use a service for stable capabilities and dependencies. Use function arguments for dynamic operation input.
- Expose explicit Layers for consumer wiring when the library owns a service implementation or resource lifecycle.
- Use scoped Layers and acquire release semantics for resources that require cleanup.
- Keep Layer construction configurable. Provide ergonomic defaults without hiding important lifecycle, persistence, concurrency, or security choices.
- Do not capture consumer specific runtime values in module globals.

## Composition And Effects

- Prefer `.pipe(...)` for readable Effect composition over nested calls.
- Do not wrap pure values or already lazy Effect constructors in unnecessary `Effect.sync`.
- Use Effect logging APIs. Do not call `console.log`, `console.warn`, or `console.error`.
- Add spans at meaningful public, I/O, workflow, and remote boundaries. Use stable names and useful structured attributes. Do not add noisy spans to trivial pure helpers.
- Use Effect concurrency primitives with explicit ownership, bounds, interruption behavior, and cleanup.
- Never fork unmanaged fibers or leave queues, latches, subscriptions, or scopes without an owner.

## Consumer API And Naming

- Optimize public APIs for low setup friction and explicit consumer control.
- Follow Effect's data first and data last conventions where they improve composition.
- Use `camelCase` for values and functions. Use `PascalCase` for types, services, schemas, and error classes.
- Suffix tagged error classes with `Error`. Give Layers and constructors names that communicate their implementation or lifecycle.
- Keep internal modules under `src/internal`. Export only deliberate consumer APIs from package entry points.
- Avoid redundant wrappers, aliases, and abstractions when an Effect primitive already expresses the contract.
- Reuse established helpers and schemas before introducing another representation of the same concept.

## Tests And Changesets

- Use Red, Green, Refactor for every bug fix.
- A bug is not confirmed until an observable regression test fails for the expected reason before the production change and passes afterward.
- Exercise production composition. Replace only true external boundaries.
- Keep tests deterministic. Use Effect test primitives for time and concurrency.
- Do not add tests that merely mirror private control flow.
- Keep one initial release changeset for all packages changed before the first release.
