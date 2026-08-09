# effect-local React Native example

A complete Expo application for `@lucas-barake/effect-local-react-native`: tasks domain,
in-process replica over expo-sqlite, reactive labels view, and a boot self test that
proves every runtime capability Effect Local depends on (WebAssembly, automerge WASM
init, crypto, base64, UTF-8, then a full replica round trip on the on-device database).

## Run it

```sh
pnpm install
pnpm start
```

Open the app in Expo Go (or a development build) on a device or simulator. The first
screen runs the self test and prints PASS/FAIL per capability.

## Verify the bundles without a device

```sh
pnpm typecheck
CI=1 pnpm exec expo export --platform ios --platform android --output-dir dist
```

The export compiles the entire stack (effect, effect-local, automerge's base64 WASM,
expo-sqlite) to Hermes bytecode for both platforms, which is what catches module
resolution and bundling regressions.

## Metro and Babel configuration

Two config pieces are load bearing, and both are annotated in place:

- [`metro.config.js`](metro.config.js) rewrites relative `.js` import specifiers to
  `.ts`. Only needed while consuming the workspace TypeScript sources; published
  builds resolve to `dist/*.js` and do not need it.
- [`babel.config.js`](babel.config.js) neutralizes the computed `import()` in effect's
  `Migrator.fromFileSystem`, which Metro rejects at bundle time. local-sql never calls
  that loader. Every consumer needs this one.
