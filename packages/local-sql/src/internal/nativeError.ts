/**
 * Keep native error construction and direct throws at one synchronous boundary. These helpers
 * preserve the native class and defect contract while the rest of the package stays in Effect.
 */

// oxlint-disable-next-line effect/noNewError -- This boundary must construct a native Error value.
export const nativeError = (message: string): Error => new Error(message)

// oxlint-disable-next-line effect/noNewError -- This boundary must construct a native TypeError value.
export const nativeTypeError = (message: string): TypeError => new TypeError(message)

export const throwDefect = (defect: unknown): never => {
  // oxlint-disable-next-line effect/noThrowStatement -- Preserve the caller's direct synchronous defect.
  throw defect
}

export const throwError = (message: string): never => throwDefect(nativeError(message))

export const throwTypeError = (message: string): never => throwDefect(nativeTypeError(message))
