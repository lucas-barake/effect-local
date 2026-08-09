/**
 * Fake of the `react-native` AppState API surface, used only in tests via the
 * vitest module alias. The `__setState` / `__listenerCount` helpers are the
 * observable boundary probes for the lifecycle tests.
 */

type AppStateStatus = "active" | "background" | "inactive" | "extension" | "unknown"

type ChangeListener = (status: AppStateStatus) => void

const changeListeners = new Set<ChangeListener>()

let currentState: AppStateStatus = "active"

export const AppState = {
  get currentState(): AppStateStatus {
    return currentState
  },
  addEventListener(type: string, listener: ChangeListener): { remove: () => void } {
    if (type === "change") changeListeners.add(listener)
    return {
      remove: () => {
        changeListeners.delete(listener)
      }
    }
  },
  __setState(next: AppStateStatus): void {
    currentState = next
    for (const listener of changeListeners) listener(next)
  },
  __listenerCount(): number {
    return changeListeners.size
  }
}
