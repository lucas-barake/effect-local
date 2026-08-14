export default {
  resolve: {
    dedupe: ["effect"]
  },
  optimizeDeps: {
    exclude: ["effect", "@effect/sql-sqlite-wasm/OpfsWorker", "@effect/wa-sqlite"]
  }
}
