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
  return { documents, byName }
}

export const get = <Documents extends ReadonlyArray<Document.Any>,>(
  self: DocumentSet<Documents>,
  name: string
): Documents[number] | undefined => self.byName.get(name)
