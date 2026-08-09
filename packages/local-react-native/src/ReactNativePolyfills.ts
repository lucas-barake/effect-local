/**
 * Runtime polyfills Effect Local needs on React Native and Hermes does not
 * provide.
 *
 * Hermes ships no `crypto.getRandomValues`, no `atob`/`btoa`, and (before
 * Hermes v1) no `TextEncoder`. Effect Local needs all three: automerge's WASM
 * build decodes its embedded base64 blob with `atob`, seeds its random
 * generator through `crypto.getRandomValues`, and encodes strings with
 * `TextEncoder`, while Effect Local itself instantiates `TextEncoder` in
 * canonical hashing and conflict handling.
 *
 * Call {@link install} once at application entry, before importing or building
 * any Effect Local layer:
 *
 * ```ts
 * import * as ReactNativePolyfills from "@lucas-barake/effect-local-react-native/ReactNativePolyfills"
 *
 * ReactNativePolyfills.install()
 * ```
 *
 * Every install is conditional: a runtime that already provides a global keeps
 * its native implementation. The `TextDecoder` fallback only implements UTF-8
 * without `fatal` semantics, which is sufficient because automerge and Effect
 * Local only ever decode their own valid UTF-8 output.
 *
 * @since 0.1.0
 */
import * as base64 from "base64-js"
import * as ExpoCrypto from "expo-crypto"

const toBinaryString = (bytes: Uint8Array): string => {
  // Chunked: spreading the full array into fromCharCode overflows the call
  // stack for megabyte-sized inputs such as automerge's embedded WASM blob.
  const CHUNK_SIZE = 0x8000
  const parts: Array<string> = []
  for (let index = 0; index < bytes.length; index += CHUNK_SIZE) {
    parts.push(String.fromCharCode(...bytes.subarray(index, index + CHUNK_SIZE)))
  }
  return parts.join("")
}

const fromBinaryString = (input: string): Uint8Array => {
  const bytes = new Uint8Array(input.length)
  for (let index = 0; index < input.length; index++) {
    bytes[index] = input.charCodeAt(index)
  }
  return bytes
}

// Native TextEncoder substitutes U+FFFD for lone surrogates; encodeURIComponent would
// throw a URIError on them instead.
const replaceLoneSurrogates = (input: string): string =>
  input.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "\uFFFD")

class TextEncoderPolyfill {
  encode(input = ""): Uint8Array {
    return fromBinaryString(unescape(encodeURIComponent(replaceLoneSurrogates(input))))
  }
  encodeInto(input: string, destination: Uint8Array): { read: number; written: number } {
    let read = 0
    let written = 0
    for (const character of input) {
      const encoded = this.encode(character)
      if (written + encoded.length > destination.length) break
      destination.set(encoded, written)
      written += encoded.length
      read += character.length
    }
    return { read, written }
  }
}

class TextDecoderPolyfill {
  decode(input?: ArrayBuffer | ArrayBufferView): string {
    if (input === undefined) return ""
    const bytes = ArrayBuffer.isView(input)
      ? new Uint8Array(input.buffer, input.byteOffset, input.byteLength)
      : new Uint8Array(input)
    // Percent-encode each byte and let decodeURIComponent reassemble the UTF-8
    // sequences; only valid for well-formed UTF-8, which is all automerge and
    // Effect Local ever decode.
    let escaped = ""
    for (let index = 0; index < bytes.length; index++) {
      escaped += "%" + bytes[index].toString(16).padStart(2, "0")
    }
    return decodeURIComponent(escaped)
  }
}

/**
 * Installs the globals Effect Local requires, keeping any the runtime already
 * provides:
 *
 * - `crypto.getRandomValues` and `crypto.randomUUID` from expo-crypto's native CSPRNG
 * - `atob` / `btoa` from chunked base64-js conversions
 * - `TextEncoder` / `TextDecoder` from minimal UTF-8 implementations
 *
 * @since 0.1.0
 */
export const install = (): void => {
  const target = globalThis as Record<string, any>

  if (typeof target.crypto !== "object" || target.crypto === null) {
    target.crypto = {}
  }
  if (typeof target.crypto.getRandomValues !== "function") {
    target.crypto.getRandomValues = ExpoCrypto.getRandomValues
  }
  if (typeof target.crypto.randomUUID !== "function") {
    target.crypto.randomUUID = ExpoCrypto.randomUUID
  }

  if (typeof target.atob !== "function") {
    target.atob = (input: string): string => toBinaryString(base64.toByteArray(input))
  }
  if (typeof target.btoa !== "function") {
    target.btoa = (input: string): string => {
      for (let index = 0; index < input.length; index++) {
        if (input.charCodeAt(index) > 255) {
          throw new Error("InvalidCharacterError: btoa input contains code units above 255")
        }
      }
      return base64.fromByteArray(fromBinaryString(input))
    }
  }

  if (typeof target.TextEncoder !== "function") {
    target.TextEncoder = TextEncoderPolyfill
  }
  if (typeof target.TextDecoder !== "function") {
    target.TextDecoder = TextDecoderPolyfill
  }
}
