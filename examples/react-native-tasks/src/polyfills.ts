// Polyfills must land before anything evaluates Effect Local or automerge: automerge
// decodes its embedded WASM with atob and seeds randomness through
// crypto.getRandomValues, and Effect Local instantiates TextEncoder at module scope.
// ESM evaluates an entry module's imports before its body, so the install call lives in
// this dedicated module and is pulled in by a bare side-effect import, which runs before
// any later import in the entry file.
import * as ReactNativePolyfills from "@lucas-barake/effect-local-react-native/ReactNativePolyfills"

ReactNativePolyfills.install()
