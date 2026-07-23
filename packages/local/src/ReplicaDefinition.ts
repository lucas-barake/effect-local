import * as Schema from "effect/Schema"
import * as Canonical from "./Canonical.js"
import type * as Document from "./Document.js"
import type * as DocumentSet from "./DocumentSet.js"
import type * as Mutation from "./Mutation.js"
import type * as Projection from "./Projection.js"
import type * as Query from "./Query.js"

export interface ReplicaDefinition<
  out Name extends string,
  out Documents extends ReadonlyArray<Document.Any>,
  Mutations extends ReadonlyArray<Mutation.Any>,
  Projections extends ReadonlyArray<Projection.Any>,
  Queries extends ReadonlyArray<Query.Any>,
> {
  readonly name: Name
  readonly documents: DocumentSet.DocumentSet<Documents>
  readonly mutations: Mutations
  readonly projections: Projections
  readonly queries: Queries
  readonly hash: string
}

export type Any = ReplicaDefinition<any, any, any, any, any>

export const invalidationKeys = (definition: Any): ReadonlyArray<string> => [
  ...definition.documents.documents.map((document: Document.Any) => document.name),
  ...definition.projections.map((projection: Projection.Any) => projection.name)
]

const assertUnique = (kind: string, values: ReadonlyArray<{ readonly name: string }>): void => {
  const names = new Set<string>()
  for (const value of values) {
    if (names.has(value.name)) throw new TypeError(`Duplicate ${kind} name: ${value.name}`)
    names.add(value.name)
  }
}

const schemaDescriptor = (schema: Document.WireSchema) => Schema.toJsonSchemaDocument(schema)

export const make = <
  const Name extends string,
  const Documents extends ReadonlyArray<Document.Any>,
  const Mutations extends ReadonlyArray<Mutation.Any> = readonly [],
  const Projections extends ReadonlyArray<Projection.Any> = readonly [],
  const Queries extends ReadonlyArray<Query.Any> = readonly [],
>(options: {
  readonly name: Name
  readonly documents: DocumentSet.DocumentSet<Documents>
  readonly mutations?: Mutations
  readonly projections?: Projections
  readonly queries?: Queries
}): ReplicaDefinition<Name, Documents, Mutations, Projections, Queries> => {
  if (options.name.length === 0) throw new TypeError("Replica definition name must be nonempty")
  const mutations = Object.freeze([...(options.mutations ?? [])]) as unknown as Mutations
  const projections = Object.freeze([...(options.projections ?? [])]) as unknown as Projections
  const queries = Object.freeze([...(options.queries ?? [])]) as unknown as Queries
  const documentSet: DocumentSet.DocumentSet<Documents> = {
    documents: options.documents.documents,
    byName: new Map(options.documents.byName)
  }
  assertUnique("mutation", mutations)
  assertUnique("projection", projections)
  assertUnique("query", queries)
  const documents = new Set(options.documents.documents)
  const registeredProjections = new Set(projections)
  for (const mutation of mutations) {
    if (!documents.has(mutation.document)) {
      throw new TypeError(`Mutation references an unknown document: ${mutation.name}`)
    }
  }
  for (const projection of projections) {
    if (!documents.has(projection.document)) {
      throw new TypeError(`Projection references an unknown document: ${projection.name}`)
    }
  }
  for (const query of queries) {
    for (const dependency of query.dependsOn) {
      if (!registeredProjections.has(dependency)) {
        throw new TypeError(`Query references an unknown projection: ${query.name}`)
      }
    }
  }
  const definitionHash = `def_${
    Canonical.hash({
      name: options.name,
      documents: options.documents.documents.map((document) => ({
        name: document.name,
        schema: schemaDescriptor(document.schema),
        version: document.version
      })),
      mutations: mutations.map((mutation) => ({
        document: mutation.document.name,
        error: schemaDescriptor(mutation.errorSchema),
        name: mutation.name,
        payload: schemaDescriptor(mutation.payloadSchema),
        success: schemaDescriptor(mutation.successSchema),
        version: mutation.version
      })),
      projections: projections.map((projection) => ({
        document: projection.document.name,
        name: projection.name,
        row: schemaDescriptor(projection.Row),
        version: projection.version
      })),
      queries: queries.map((query) => ({
        dependencies: query.dependsOn.map((projection: Projection.Any) => projection.name),
        error: schemaDescriptor(query.errorSchema),
        name: query.name,
        payload: schemaDescriptor(query.payloadSchema),
        success: schemaDescriptor(query.successSchema),
        version: query.version
      }))
    })
  }`
  return { ...options, documents: documentSet, mutations, projections, queries, hash: definitionHash }
}
