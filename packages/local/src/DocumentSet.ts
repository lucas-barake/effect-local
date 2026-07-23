import type * as Document from "./Document.js"

export interface DocumentSet<out Documents extends ReadonlyArray<Document.Any>,> {
  readonly documents: Documents
  readonly byName: ReadonlyMap<string, Documents[number]>
}

export const make = <const Documents extends ReadonlyArray<Document.Any>,>(
  ...documents: Documents
): DocumentSet<Documents> => {
  const byName = new Map<string, Documents[number]>()
  for (const document of documents) {
    if (byName.has(document.name)) throw new TypeError(`Duplicate document name: ${document.name}`)
    byName.set(document.name, document)
  }
  return { documents: Object.freeze(documents), byName }
}

export function get<
  Documents extends ReadonlyArray<Document.Any>,
  Name extends Documents[number]["name"],
>(
  self: DocumentSet<Documents>,
  name: Name
): Extract<Documents[number], { readonly name: Name }> | undefined
export function get<Documents extends ReadonlyArray<Document.Any>,>(
  self: DocumentSet<Documents>,
  name: string
): Documents[number] | undefined
export function get(
  self: DocumentSet<ReadonlyArray<Document.Any>>,
  name: string
): Document.Any | undefined {
  return self.byName.get(name)
}
