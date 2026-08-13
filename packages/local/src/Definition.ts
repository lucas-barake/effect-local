import * as Canonical from "./Canonical.js"
import * as Identity from "./Identity.js"
import * as Defect from "./internal/defect.js"
import type * as Model from "./Model.js"
import type * as Mutation from "./Mutation.js"
import type * as Query from "./Query.js"
import * as SchemaDescriptor from "./SchemaDescriptor.js"

export interface Definition<
  Models extends ReadonlyArray<Model.Any>,
  Mutations extends ReadonlyArray<Mutation.Any>,
  Queries extends ReadonlyArray<Query.Any>,
> {
  readonly models: Models
  readonly mutations: Mutations
  readonly queries: Queries
  readonly modelByName: ReadonlyMap<string, Model.Any>
  readonly mutationByName: ReadonlyMap<string, Mutation.Any>
  readonly queryByName: ReadonlyMap<string, Query.Any>
  readonly version: Identity.SchemaVersion
  readonly schemaIdentity: Identity.SchemaIdentity
  readonly hash: string
  readonly indexLayoutHash: string
}

const indexed = <A extends { readonly name: string },>(kind: string, values: ReadonlyArray<A>) => {
  const result = new Map<string, A>()
  for (const value of values) {
    if (result.has(value.name)) return Defect.invalid(`Duplicate ${kind} name: ${value.name}`)
    result.set(value.name, value)
  }
  return result
}

const byName = <A extends { readonly name: string },>(left: A, right: A): number => {
  if (left.name < right.name) return -1
  if (left.name > right.name) return 1
  return 0
}

export function make<
  const Models extends ReadonlyArray<Model.Any>,
  const Mutations extends ReadonlyArray<Mutation.Any>,
  const Queries extends ReadonlyArray<Query.Any> = readonly [],
>(options: {
  readonly version: number
  readonly models: Models
  readonly mutations: Mutations
  readonly queries?: Queries
}): Definition<Models, Mutations, Queries>
export function make(options: {
  readonly version: number
  readonly models: ReadonlyArray<Model.Any>
  readonly mutations: ReadonlyArray<Mutation.Any>
  readonly queries?: ReadonlyArray<Query.Any>
}): Definition<ReadonlyArray<Model.Any>, ReadonlyArray<Mutation.Any>, ReadonlyArray<Query.Any>> {
  const queries = options.queries ?? []
  const modelByName = indexed("model", options.models)
  const mutationByName = indexed("mutation", options.mutations)
  const queryByName = indexed("query", queries)
  const version = Identity.SchemaVersion.make(options.version)
  const models = options.models.map((model) => ({
    name: model.name,
    version: model.version,
    key: SchemaDescriptor.make(model.key),
    schema: SchemaDescriptor.make(model.schema)
  })).toSorted(byName)
  const mutations = options.mutations.map((mutation) => ({
    name: mutation.name,
    version: mutation.version,
    payload: SchemaDescriptor.make(mutation.payloadSchema),
    success: SchemaDescriptor.make(mutation.successSchema),
    rejection: SchemaDescriptor.make(mutation.rejectionSchema)
  })).toSorted(byName)
  const schemaHash = Canonical.hash({ format: 1, models, mutations })
  const schemaIdentity = Identity.SchemaIdentity.make({
    version,
    hash: Identity.SchemaHash.make(schemaHash)
  })
  const hash = Canonical.hash({
    format: 2,
    schemaIdentity,
    queries: queries.map((query) => ({
      name: query.name,
      payload: SchemaDescriptor.make(query.payloadSchema),
      success: SchemaDescriptor.make(query.successSchema),
      error: SchemaDescriptor.make(query.errorSchema)
    })).toSorted(byName)
  })
  const indexLayoutHash = Canonical.hash({
    format: 1,
    indexes: options.models.flatMap((model) =>
      Object.entries(model.indexes).map(([name, index]) => ({
        model: model.name,
        name,
        version: index.version,
        partition: index.partition.map((component) => ({
          name: component.name,
          affinity: component.affinity,
          schema: SchemaDescriptor.make(component.schema)
        })),
        sort: index.sort.map((component) => ({
          name: component.name,
          affinity: component.affinity,
          schema: SchemaDescriptor.make(component.schema)
        }))
      }))
    ).toSorted((left, right) => {
      const leftName = `${left.model}\u0000${left.name}`
      const rightName = `${right.model}\u0000${right.name}`
      if (leftName < rightName) return -1
      if (leftName > rightName) return 1
      return 0
    })
  })
  return Object.freeze({
    models: Object.freeze([...options.models]),
    mutations: Object.freeze([...options.mutations]),
    queries: Object.freeze([...queries]),
    modelByName,
    mutationByName,
    queryByName,
    version,
    schemaIdentity,
    hash,
    indexLayoutHash
  })
}

export interface Any
  extends Definition<ReadonlyArray<Model.Any>, ReadonlyArray<Mutation.Any>, ReadonlyArray<Query.Any>>
{}
