import { ExpoCrypto, ExpoCryptoAES } from "./nativeCrypto.js"
import { ExpoSQLite } from "./nativeSqlite.js"

const modules: Record<string, unknown> = { ExpoSQLite, ExpoCrypto, ExpoCryptoAES }

export class NativeModule {}

export const requireNativeModule = (name: string) => {
  const module = modules[name]
  if (module === undefined) throw new Error(`Native module ${name} is not faked`)
  return module
}
