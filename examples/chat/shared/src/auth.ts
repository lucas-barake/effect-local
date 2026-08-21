import * as Schema from "effect/Schema"
import { UserId } from "./domain.js"

/** Shared auth wire contracts between the login endpoint and the client. */

export const LoginRequest = Schema.Struct({
  username: Schema.String,
  password: Schema.String
})
export type LoginRequest = typeof LoginRequest.Type

export const LoginResponse = Schema.Struct({
  token: Schema.String,
  userId: UserId,
  name: Schema.String,
  color: Schema.String
})
export type LoginResponse = typeof LoginResponse.Type

/** What the server authenticator resolves a bearer token into. */
export const Principal = Schema.Struct({
  userId: UserId,
  name: Schema.String
})
export type Principal = typeof Principal.Type
