export const invalid = (message: string): never => {
  // oxlint-disable-next-line effect/noThrowStatement, effect/noNewError -- Definition constructors are synchronous and must reject an invalid value by throwing before any Effect exists.
  throw new TypeError(message)
}
