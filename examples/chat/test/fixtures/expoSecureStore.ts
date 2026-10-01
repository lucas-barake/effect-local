export const keychain = {
  entries: new Map<string, string>(),
  failWrites: false
}

const unavailable = () => Promise.reject(new Error("The keychain is unavailable"))

export const getItemAsync = (key: string): Promise<string | null> => Promise.resolve(keychain.entries.get(key) ?? null)

export const setItemAsync = (key: string, value: string): Promise<void> => {
  if (keychain.failWrites) return unavailable()
  keychain.entries.set(key, value)
  return Promise.resolve()
}

export const deleteItemAsync = (key: string): Promise<void> => {
  if (keychain.failWrites) return unavailable()
  keychain.entries.delete(key)
  return Promise.resolve()
}
