export const nativeError = (message: string): Error =>
  // oxlint-disable-next-line effect/noNewError -- These tests intentionally exercise native Error transport.
  new Error(message)

export const throwDefect = (defect: unknown): never => {
  // oxlint-disable-next-line effect/noThrowStatement -- These tests intentionally exercise hostile synchronous callbacks and getters.
  throw defect
}
