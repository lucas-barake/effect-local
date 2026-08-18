import {
  ConversationId,
  ConversationReadState,
  dmConversationId,
  groupConversationId,
  Message,
  MessageId,
  readStateKey,
  tickState,
  UserId
} from "@effect-local/example-chat-shared/domain"
import { assert, describe, it } from "@effect/vitest"

const alice = UserId.make("alice")
const bob = UserId.make("bob")
const carol = UserId.make("carol")

const message = (overrides?: Partial<Message>): Message =>
  Message.schema.make({
    id: MessageId.make("msg_test"),
    conversationId: dmConversationId(alice, bob),
    senderId: alice,
    text: "hello",
    createdAt: 1_000,
    ...overrides
  })

const readState = (
  conversationId: ConversationId,
  userId: UserId,
  deliveredUpTo: number,
  readUpTo: number
): ConversationReadState => ConversationReadState.schema.make({ conversationId, userId, deliveredUpTo, readUpTo })

describe("conversation ids", () => {
  it("dm ids are order-independent", () => {
    assert.strictEqual(dmConversationId(alice, bob), dmConversationId(bob, alice))
    assert.strictEqual(dmConversationId(alice, bob), "dm:alice:bob")
  })

  it("dm ids differ per pair and from the group id", () => {
    assert.notStrictEqual(dmConversationId(alice, bob), dmConversationId(alice, carol))
    assert.strictEqual(groupConversationId, "group:everyone")
  })
})

describe("readStateKey", () => {
  it("is unambiguous when components contain the separator", () => {
    // A naive `${conversationId}:${userId}` join would collide here.
    const left = readStateKey(ConversationId.make("dm:alice:bob"), carol)
    const right = readStateKey(ConversationId.make("dm:alice"), UserId.make("bob:carol"))
    assert.notStrictEqual(left, right)
  })
})

describe("tickState", () => {
  const members: ReadonlyArray<UserId> = [alice, bob]
  const sent = message()

  it("failed wins over every other state", () => {
    assert.strictEqual(
      tickState({
        failed: true,
        pending: true,
        message: sent,
        senderId: alice,
        readStates: [readState(sent.conversationId, bob, 2_000, 2_000)],
        memberIds: members
      }),
      "failed"
    )
  })

  it("pending while the mutation sits in the local outbox", () => {
    assert.strictEqual(
      tickState({ failed: false, pending: true, message: sent, senderId: alice, readStates: [], memberIds: members }),
      "pending"
    )
  })

  it("sent once accepted with no peer coverage", () => {
    assert.strictEqual(
      tickState({ failed: false, pending: false, message: sent, senderId: alice, readStates: [], memberIds: members }),
      "sent"
    )
  })

  it("delivered only once every other member has delivered past createdAt", () => {
    const groupMembers: ReadonlyArray<UserId> = [alice, bob, carol]
    const partial = tickState({
      failed: false,
      pending: false,
      message: sent,
      senderId: alice,
      readStates: [readState(sent.conversationId, bob, 2_000, 0)],
      memberIds: groupMembers
    })
    assert.strictEqual(partial, "sent")
    const full = tickState({
      failed: false,
      pending: false,
      message: sent,
      senderId: alice,
      readStates: [
        readState(sent.conversationId, bob, 2_000, 0),
        readState(sent.conversationId, carol, 1_500, 0)
      ],
      memberIds: groupMembers
    })
    assert.strictEqual(full, "delivered")
  })

  it("read requires every other member's readUpTo; own rows are ignored", () => {
    const withOwnRowOnly = tickState({
      failed: false,
      pending: false,
      message: sent,
      senderId: alice,
      readStates: [readState(sent.conversationId, alice, 9_999, 9_999)],
      memberIds: members
    })
    assert.strictEqual(withOwnRowOnly, "sent")
    const peerRead = tickState({
      failed: false,
      pending: false,
      message: sent,
      senderId: alice,
      readStates: [readState(sent.conversationId, bob, 2_000, 2_000)],
      memberIds: members
    })
    assert.strictEqual(peerRead, "read")
  })

  it("a peer row from a different conversation does not count", () => {
    const other = readState(ConversationId.make("dm:alice:carol"), bob, 9_999, 9_999)
    assert.strictEqual(
      tickState({
        failed: false,
        pending: false,
        message: sent,
        senderId: alice,
        readStates: [other],
        memberIds: members
      }),
      "sent"
    )
  })
})
