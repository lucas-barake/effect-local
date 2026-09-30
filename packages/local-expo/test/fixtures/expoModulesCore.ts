export { requireNativeModule } from "./expo.js"

export class CodedError extends Error {
  readonly code: string
  constructor(code: string, message: string) {
    super(message)
    this.code = code
  }
}

export class UnavailabilityError extends CodedError {
  constructor(moduleName: string, propertyName: string) {
    super("ERR_UNAVAILABLE", `The method or property ${moduleName}.${propertyName} is not available`)
  }
}
