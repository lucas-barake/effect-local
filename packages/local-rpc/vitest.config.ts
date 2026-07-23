import { defineProject } from "vitest/config"

export default defineProject({
  test: {
    name: "local-rpc",
    include: ["test/**/*.test.ts"],
    benchmark: {
      exclude: [
        "**/node_modules/**",
        "**/dist/**",
        "**/cypress/**",
        "**/.{idea,git,cache,output,temp}/**",
        "**/{karma,rollup,webpack,vite,vitest,jest,ava,babel,nyc,cypress,tsup,build,eslint,prettier}.config.*",
        "bench/PeerRpcServerPerformance.bench.ts"
      ]
    }
  }
})
