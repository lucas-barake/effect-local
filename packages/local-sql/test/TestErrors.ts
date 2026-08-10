// oxlint-disable-next-line effect/noNewError -- Test defects must preserve native Error identity.
export const nativeError = (message: string) => new Error(message)

// oxlint-disable-next-line effect/noNewError -- Test defects must preserve native TypeError identity.
export const nativeTypeError = (message: string) => new TypeError(message)
