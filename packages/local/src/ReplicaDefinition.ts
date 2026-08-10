import * as Effect from "effect/Effect"
import * as Canonical from "./Canonical.js"
import type * as Document from "./Document.js"
import type * as DocumentSet from "./DocumentSet.js"
import type * as Identity from "./Identity.js"
import type * as Mutation from "./Mutation.js"
import type * as Projection from "./Projection.js"
import type * as Query from "./Query.js"
import * as SchemaDescriptor from "./SchemaDescriptor.js"
import type * as Transient from "./Transient.js"

export interface ReplicaDefinition<
  out Name extends string,
  out Documents extends ReadonlyArray<Document.Any>,
  Mutations extends ReadonlyArray<Mutation.Any>,
  Projections extends ReadonlyArray<Projection.Any>,
  Queries extends ReadonlyArray<Query.Any>,
  Transients extends ReadonlyArray<Transient.Any>,
> {
  readonly name: Name
  readonly documents: DocumentSet.DocumentSet<Documents>
  readonly mutations: Mutations
  readonly projections: Projections
  readonly queries: Queries
  readonly transients: Transients
  readonly hash: string
}

export type Any = ReplicaDefinition<any, any, any, any, any, any>

const invalidationKeyDomain = "@lucas-barake/effect-local/invalidation"

export const documentTypeQueryKey = (documentType: string): string => documentType

export const documentInstanceKey = (
  documentType: string,
  documentId: Identity.DocumentId
): string => Canonical.stringify([invalidationKeyDomain, "document-instance", documentType, documentId])

export const documentTypeRefreshKey = (documentType: string): string =>
  Canonical.stringify([invalidationKeyDomain, "document-type-refresh", documentType])

export const documentCommitKeys = (
  documentType: string,
  documentId: Identity.DocumentId
): ReadonlyArray<string> => [
  documentTypeQueryKey(documentType),
  documentInstanceKey(documentType, documentId)
]

export const invalidationKeys = (definition: Any): ReadonlyArray<string> =>
  Array.from(
    new Set([
      ...definition.documents.documents.map((document: Document.Any) => documentTypeQueryKey(document.name)),
      ...definition.projections.map((projection: Projection.Any) => projection.name),
      ...definition.documents.documents.map((document: Document.Any) => documentTypeRefreshKey(document.name))
    ])
  )

const assertUnique = (kind: string, values: ReadonlyArray<{ readonly name: string }>): void => {
  const names = new Set<string>()
  for (const value of values) {
    if (names.has(value.name)) {
      Effect.runSync(Effect.die(new TypeError(`Duplicate ${kind} name: ${value.name}`)))
    }
    names.add(value.name)
  }
}

const assertKnownDocuments = (
  kind: string,
  values: ReadonlyArray<{ readonly name: string; readonly document: Document.Any }>,
  documents: ReadonlySet<Document.Any>
): void => {
  for (const value of values) {
    if (!documents.has(value.document)) {
      Effect.runSync(Effect.die(new TypeError(`${kind} references an unknown document: ${value.name}`)))
    }
  }
}

export function make<
  const Name extends string,
  const Documents extends ReadonlyArray<Document.Any>,
  const Mutations extends ReadonlyArray<Mutation.Any> = readonly [],
  const Projections extends ReadonlyArray<Projection.Any> = readonly [],
  const Queries extends ReadonlyArray<Query.Any> = readonly [],
  const Transients extends ReadonlyArray<Transient.Any> = readonly [],
>(options: {
  readonly name: Name
  readonly documents: DocumentSet.DocumentSet<Documents>
  readonly mutations?: Mutations
  readonly projections?: Projections
  readonly queries?: Queries
  readonly transients?: Transients
}): ReplicaDefinition<Name, Documents, Mutations, Projections, Queries, Transients>
export function make<
  const Name extends string,
  const Documents extends ReadonlyArray<Document.Any>,
  const Mutations extends ReadonlyArray<Mutation.Any> = readonly [],
  const Projections extends ReadonlyArray<Projection.Any> = readonly [],
  const Queries extends ReadonlyArray<Query.Any> = readonly [],
  const Transients extends ReadonlyArray<Transient.Any> = readonly [],
>(options: {
  readonly name: Name
  readonly documents: DocumentSet.DocumentSet<Documents>
  readonly mutations?: Mutations
  readonly projections?: Projections
  readonly queries?: Queries
  readonly transients?: Transients
}): any {
  if (options.name.length === 0) Effect.runSync(Effect.die(new TypeError("Replica definition name must be nonempty")))
  const mutations = Object.freeze([...(options.mutations ?? [])])
  const projections = Object.freeze([...(options.projections ?? [])])
  const queries = Object.freeze([...(options.queries ?? [])])
  const transients = Object.freeze([...(options.transients ?? [])])
  const documentSet = {
    documents: Object.freeze([...options.documents.documents]),
    byName: new Map(options.documents.byName)
  }
  assertUnique("mutation", mutations)
  assertUnique("projection", projections)
  assertUnique("query", queries)
  assertUnique("transient", transients)
  const documents = new Set(documentSet.documents)
  const registeredProjections: Set<Projection.Any> = new Set(projections)
  assertKnownDocuments("Mutation", mutations, documents)
  assertKnownDocuments("Projection", projections, documents)
  for (const query of queries) {
    for (const dependency of query.dependsOn) {
      if (!registeredProjections.has(dependency)) {
        Effect.runSync(Effect.die(new TypeError(`Query references an unknown projection: ${query.name}`)))
      }
    }
  }
  assertKnownDocuments("Transient", transients, documents)
  const transientHash: { transients?: ReadonlyArray<unknown> } = {}
  if (transients.length > 0) {
    transientHash.transients = transients.map((transient) => ({
      document: transient.document.name,
      name: transient.name,
      payload: SchemaDescriptor.make(transient.payloadSchema)
    }))
  }
  const definitionHash = `def_${
    Canonical.hash({
      name: options.name,
      documents: documentSet.documents.map((document) => ({
        name: document.name,
        schema: SchemaDescriptor.make(document.schema),
        version: document.version
      })),
      mutations: mutations.map((mutation) => ({
        document: mutation.document.name,
        error: SchemaDescriptor.make(mutation.errorSchema),
        name: mutation.name,
        payload: SchemaDescriptor.make(mutation.payloadSchema),
        success: SchemaDescriptor.make(mutation.successSchema),
        version: mutation.version
      })),
      projections: projections.map((projection) => ({
        document: projection.document.name,
        name: projection.name,
        row: SchemaDescriptor.make(projection.Row),
        version: projection.version
      })),
      queries: queries.map((query) => ({
        dependencies: query.dependsOn.map((projection: Projection.Any) => projection.name),
        error: SchemaDescriptor.make(query.errorSchema),
        name: query.name,
        payload: SchemaDescriptor.make(query.payloadSchema),
        success: SchemaDescriptor.make(query.successSchema),
        version: query.version
      })),
      ...transientHash
    })
  }`
  return { ...options, documents: documentSet, mutations, projections, queries, transients, hash: definitionHash }
}
