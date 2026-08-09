// Metro statically analyzes import() calls and rejects any whose argument is not a
// string literal. effect's `Migrator.fromFileSystem` (a Node-only loader that local-sql
// never calls) contains a computed import, which fails the whole bundle at transform
// time. Rewrite that one call site to a lazy rejection.
const neutralizeComputedMigratorImport = () => ({
  visitor: {
    CallExpression(path, state) {
      const filename = state.filename ?? ""
      if (!/effect[\\/]dist[\\/]unstable[\\/]sql[\\/]Migrator\.js$/.test(filename)) return
      if (path.node.callee.type !== "Import") return
      const [argument] = path.node.arguments
      if (argument === undefined || argument.type === "StringLiteral") return
      path.replaceWithSourceString(
        "Promise.reject(new Error('Migrator.fromFileSystem is not supported on React Native'))"
      )
    }
  }
})

module.exports = function(api) {
  api.cache(true)
  return {
    presets: ["babel-preset-expo"],
    plugins: [neutralizeComputedMigratorImport]
  }
}
