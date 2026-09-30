import * as Effect from "effect/Effect"
import * as HttpClient from "effect/http/HttpClient"
import * as HttpClientRequest from "effect/http/HttpClientRequest"
import * as HttpClientResponse from "effect/http/HttpClientResponse"
import * as Schema from "effect/Schema"
import { type LoginRequest, LoginResponse } from "./auth.js"

export const sessionKey = "effect-local-chat:session"

export const StoredSession = Schema.NullOr(LoginResponse)

export class LoginFailed extends Schema.TaggedError<LoginFailed>(
  "@effect-local/example-chat/LoginFailed"
)("LoginFailed", { message: Schema.String }) {}

export const requestLogin = Effect.fn("chat.login")(
  function*(serverUrl: string, credentials: LoginRequest) {
    const client = yield* HttpClient.HttpClient
    const request = yield* HttpClientRequest.post(`${serverUrl}/login`).pipe(
      HttpClientRequest.bodyJson(credentials)
    )
    const response = yield* client.execute(request)
    if (response.status === 401) {
      return yield* new LoginFailed({ message: "Invalid username or password" })
    }
    const ok = yield* HttpClientResponse.filterStatusOk(response)
    return yield* HttpClientResponse.schemaBodyJson(LoginResponse)(ok)
  },
  Effect.catchTags({
    HttpBodyError: () => new LoginFailed({ message: "Could not encode the login request" }),
    HttpClientError: () => new LoginFailed({ message: "Login service unavailable" }),
    SchemaError: () => new LoginFailed({ message: "Malformed login response" })
  })
)
