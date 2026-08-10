export const nativeError = (message: string): Error =>
  // oxlint-disable-next-line effect/noNewError -- These tests intentionally exercise native Error transport.
  new Error(message)

export const nativeTypeError = (message: string): TypeError =>
  // oxlint-disable-next-line effect/noNewError -- These tests intentionally preserve the native TypeError defect shape.
  new TypeError(message)

export const throwDefect = (defect: unknown): never => {
  // oxlint-disable-next-line effect/noThrowStatement -- These tests intentionally exercise hostile synchronous callbacks and getters.
  throw defect
}
