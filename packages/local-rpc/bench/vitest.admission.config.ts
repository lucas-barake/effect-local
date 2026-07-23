import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    name: "local-rpc-admission",
    benchmark: { include: ["packages/local-rpc/bench/PeerRpcServerPerformance.bench.ts"] }
  }
})
