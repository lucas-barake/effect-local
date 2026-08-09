# @lucas-barake/effect-local-react-native

React Native (Expo) support for Effect Local: an expo-sqlite backed Effect `SqlClient`
driver, an expo-crypto backed `Crypto` layer, Hermes runtime polyfills, AppState
lifecycle integration, a WebSocket layer, and Effect Atom bindings.

The replica runs in-process; none of the browser package's worker or ownership machinery
applies on a single-threaded app. Requires React Native >= 0.85 (Expo SDK >= 56, Hermes
v1 with WebAssembly for automerge) and `ReactNativePolyfills.install()` at application
entry.

Import the neutral Atom factories from `@lucas-barake/effect-local-sql/ReplicaAtom`. The
`@lucas-barake/effect-local-react-native/ReplicaAtom` subpath remains as a convenience
reexport.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme)
for the React Native composition guide, platform requirements, and API reference, and
[`examples/react-native-tasks`](https://github.com/lucas-barake/effect-local/tree/main/examples/react-native-tasks)
for a complete checked Expo application.
