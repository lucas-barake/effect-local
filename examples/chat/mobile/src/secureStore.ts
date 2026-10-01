import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as KeyValueStore from "effect/persistence/KeyValueStore"
import * as SecureStore from "expo-secure-store"

export const layerSecureStore = Layer.sync(KeyValueStore.KeyValueStore, () =>
  KeyValueStore.makeStringOnly({
    get: (key) =>
      Effect.tryPromise({
        try: () => SecureStore.getItemAsync(key),
        catch: (cause) =>
          new KeyValueStore.KeyValueStoreError({ method: "get", key, message: `Unable to read ${key}`, cause })
      }).pipe(Effect.map((value) => value ?? undefined)),
    set: (key, value) =>
      Effect.tryPromise({
        try: () => SecureStore.setItemAsync(key, value),
        catch: (cause) =>
          new KeyValueStore.KeyValueStoreError({ method: "set", key, message: `Unable to write ${key}`, cause })
      }),
    remove: (key) =>
      Effect.tryPromise({
        try: () => SecureStore.deleteItemAsync(key),
        catch: (cause) =>
          new KeyValueStore.KeyValueStoreError({ method: "remove", key, message: `Unable to remove ${key}`, cause })
      }),
    clear: Effect.fail(
      new KeyValueStore.KeyValueStoreError({
        method: "clear",
        message: "expo-secure-store cannot enumerate its entries"
      })
    ),
    size: Effect.fail(
      new KeyValueStore.KeyValueStoreError({
        method: "size",
        message: "expo-secure-store cannot enumerate its entries"
      })
    )
  }))
