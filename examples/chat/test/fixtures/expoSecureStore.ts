import { nativeModules } from "../../../../packages/local-expo/test/fixtures/expo.js"

export const keychain = {
  entries: new Map<string, string>(),
  failWrites: false
}

const unavailable = () => Promise.reject(new Error("The keychain is unavailable"))

nativeModules.ExpoSecureStore = {
  getValueWithKeyAsync: (key: string): Promise<string | null> => Promise.resolve(keychain.entries.get(key) ?? null),
  setValueWithKeyAsync: (value: string, key: string): Promise<void> => {
    if (keychain.failWrites) return unavailable()
    keychain.entries.set(key, value)
    return Promise.resolve()
  },
  deleteValueWithKeyAsync: (key: string): Promise<void> => {
    if (keychain.failWrites) return unavailable()
    keychain.entries.delete(key)
    return Promise.resolve()
  }
}
