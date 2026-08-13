import {
  isArrayLiteralExpression,
  isExpression,
  isIdentifier,
  isObjectLiteralExpression,
  isTypeNode
} from "@typescript/native-preview/unstable/ast/is"
import { API, SignatureKind, SymbolFlags, TypeFlags } from "@typescript/native-preview/unstable/sync"
import { pipe } from "effect/Function"
import { realpathSync } from "node:fs"
import { resolve } from "node:path"

const effectMarker = "~effect/Effect"

export const makeTaggedEffectErrorChecker = ({ cwd }) => {
  const effectUnhandledDeclaration = pipe(
    resolve(cwd, "node_modules/effect/dist/Types.d.ts"),
    (path) => realpathSync(path)
  )
  const effectUnhandledDeclarations = new Set([
    effectUnhandledDeclaration,
    effectUnhandledDeclaration.toLowerCase()
  ])
  const effectSocketDeclaration = pipe(
    resolve(cwd, "node_modules/effect/dist/unstable/socket/Socket.d.ts"),
    (path) => realpathSync(path)
  )
  const effectSocketDeclarations = new Set([
    effectSocketDeclaration,
    effectSocketDeclaration.toLowerCase()
  ])
  const sourceOverrides = new Map()
  let openFilename
  const api = new API({
    cwd,
    fs: {
      readFile: (candidate) => {
        const absoluteCandidate = resolve(candidate)
        if (sourceOverrides.has(absoluteCandidate)) return sourceOverrides.get(absoluteCandidate)
        return undefined
      }
    }
  })

  const find = ({ filename, sourceText }) => {
    const absoluteFilename = resolve(cwd, filename)
    sourceOverrides.set(absoluteFilename, sourceText)
    let snapshot
    try {
      const previousFilename = openFilename
      let update
      if (previousFilename === absoluteFilename) {
        const closingSnapshot = api.updateSnapshot({
          closeFiles: [absoluteFilename],
          fileChanges: { changed: [absoluteFilename] }
        })
        closingSnapshot.dispose()
        openFilename = undefined
        update = { openFiles: [absoluteFilename] }
      } else if (previousFilename === undefined) {
        update = { openFiles: [absoluteFilename] }
      } else {
        update = { openFiles: [absoluteFilename], closeFiles: [previousFilename] }
      }
      snapshot = api.updateSnapshot(update)
      openFilename = absoluteFilename
      const project = snapshot.getDefaultProjectForFile(absoluteFilename)
      if (project === undefined) throw new Error(`No TypeScript project contains ${absoluteFilename}`)
      const sourceFile = project.program.getSourceFile(absoluteFilename)
      if (sourceFile === undefined) throw new Error(`TypeScript did not load ${absoluteFilename}`)
      const checker = project.checker
      const expressions = []
      const visit = (node) => {
        const typeNode = isTypeNode(node)
        const namedNode = isIdentifier(node) && node.parent.name === node
        const literalContainer = isArrayLiteralExpression(node) || isObjectLiteralExpression(node)
        if (
          (isExpression(node) || typeNode) &&
          !namedNode &&
          !literalContainer
        ) {
          expressions.push(node)
        }
        node.forEachChild(visit)
      }
      visit(sourceFile)

      const expressionTypes = checker.getTypeAtLocation(expressions)
      const typeByExpression = new Map()
      for (let index = 0; index < expressions.length; index++) {
        const type = expressionTypes[index]
        if (type !== undefined) typeByExpression.set(expressions[index], type)
      }

      const effectErrorByType = new Map()
      const getEffectError = (type, expression) => {
        if (effectErrorByType.has(type.id)) return effectErrorByType.get(type.id)
        const markerSymbol = checker.getPropertyOfType(type, effectMarker)
        if (markerSymbol === undefined) {
          effectErrorByType.set(type.id, undefined)
          return undefined
        }
        const markerType = checker.getTypeOfSymbolAtLocation(markerSymbol, expression)
        const errorSymbol = checker.getPropertyOfType(markerType, "_E")
        if (errorSymbol === undefined) {
          throw new Error(`The Effect marker on ${absoluteFilename} has no _E channel`)
        }
        const errorAccessor = checker.getTypeOfSymbolAtLocation(errorSymbol, expression)
        const signatures = checker.getSignaturesOfType(errorAccessor, SignatureKind.Call)
        if (signatures.length !== 1) {
          throw new Error(`The Effect _E marker on ${absoluteFilename} is not a single callable signature`)
        }
        const errorType = checker.getReturnTypeOfSignature(signatures[0])
        if (errorType === undefined) {
          throw new Error(`TypeScript could not resolve an Effect error channel in ${absoluteFilename}`)
        }
        effectErrorByType.set(type.id, errorType)
        return errorType
      }

      const untaggedByType = new Map()
      const isEffectUnhandled = (type) => {
        const symbols = [type.getSymbol(), type.getAliasSymbol()].filter((symbol) => symbol !== undefined)
        return symbols.some((symbol) =>
          symbol.name === "unhandled" &&
          symbol.declarations.some((declaration) => effectUnhandledDeclarations.has(declaration.path))
        )
      }
      const isEffectSocketRunRawError = (type) => {
        const symbol = type.getSymbol()
        if (symbol?.name !== "E") return false
        return symbol.declarations.some((declaration) => {
          if (!effectSocketDeclarations.has(declaration.path)) return false
          const node = declaration.resolve(project)
          const runRaw = node?.parent?.parent
          const options = runRaw?.parent?.parent
          const make = options?.parent?.parent
          return runRaw?.name?.text === "runRaw" && options?.name?.text === "options" && make?.name?.text === "make"
        })
      }
      const getUntaggedMembers = (type, seen = new Set()) => {
        if (untaggedByType.has(type.id)) return untaggedByType.get(type.id)
        if (seen.has(type.id)) return [type]
        seen.add(type.id)

        let untagged
        if ((type.flags & TypeFlags.Never) !== 0 || isEffectUnhandled(type)) {
          untagged = []
        } else if ((type.flags & TypeFlags.Union) !== 0) {
          untagged = []
          const members = type.getTypes()
          for (const member of members) {
            untagged.push(...getUntaggedMembers(member, seen))
          }
        } else if ((type.flags & TypeFlags.TypeParameter) !== 0) {
          if (isEffectSocketRunRawError(type)) {
            untagged = []
          } else {
            const constraint = checker.getBaseConstraintOfType(type)
            if (constraint === undefined) {
              untagged = [type]
            } else {
              untagged = getUntaggedMembers(constraint, seen)
            }
          }
        } else {
          const tag = checker.getPropertyOfType(type, "_tag")
          if (tag === undefined || (tag.flags & SymbolFlags.Optional) !== 0) {
            untagged = [type]
          } else {
            untagged = []
          }
        }

        seen.delete(type.id)
        untaggedByType.set(type.id, untagged)
        return untagged
      }

      const violationByExpression = new Map()
      for (const expression of expressions) {
        const type = typeByExpression.get(expression)
        if (type === undefined) continue
        const errorType = getEffectError(type, expression)
        if (errorType === undefined) continue
        const untagged = getUntaggedMembers(errorType)
        if (untagged.length === 0) continue
        const memberIds = new Set(untagged.map((member) => member.id))
        violationByExpression.set(expression, { errorType, memberIds, untagged })
      }

      const descendantMemberIdsByExpression = new Map()
      const collectViolationMemberIds = (node) => {
        let descendantMemberIds
        node.forEachChild((child) => {
          const childMemberIds = collectViolationMemberIds(child)
          if (childMemberIds === undefined) return
          descendantMemberIds ??= new Set()
          for (const memberId of childMemberIds) descendantMemberIds.add(memberId)
        })

        const violation = violationByExpression.get(node)
        if (violation === undefined) return descendantMemberIds
        if (descendantMemberIds !== undefined) {
          descendantMemberIdsByExpression.set(node, new Set(descendantMemberIds))
        }
        const subtreeMemberIds = descendantMemberIds ?? new Set()
        for (const memberId of violation.memberIds) subtreeMemberIds.add(memberId)
        return subtreeMemberIds
      }
      collectViolationMemberIds(sourceFile)

      const diagnostics = []
      for (const expression of expressions) {
        const violation = violationByExpression.get(expression)
        if (violation === undefined) continue

        const descendantMemberIds = descendantMemberIdsByExpression.get(expression)
        const innermostMembers = violation.untagged.filter(
          (member) => descendantMemberIds?.has(member.id) !== true
        )
        if (innermostMembers.length === 0) continue

        const names = innermostMembers.map((member) => checker.typeToString(member))
        diagnostics.push({
          start: expression.getStart(sourceFile),
          end: expression.getEnd(),
          errorTypes: [...new Set(names)].toSorted((left, right) => left.localeCompare(right))
        })
      }
      return diagnostics
    } finally {
      if (snapshot !== undefined) snapshot.dispose()
      sourceOverrides.delete(absoluteFilename)
    }
  }

  return {
    find,
    close: () => {
      api.close()
      openFilename = undefined
      sourceOverrides.clear()
    }
  }
}
