export const throwTypeError = (message: string): never => {
  // oxlint-disable-next-line effect/noThrowStatement, effect/noNewError -- synchronous public constructors must preserve their direct TypeError contract
  throw new TypeError(message)
}
