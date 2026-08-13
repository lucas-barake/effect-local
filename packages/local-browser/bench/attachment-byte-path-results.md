# Attachment byte path results

Run on 2026-08-13 in T3Code Chromium 146.0.7680.216 on macOS. The harness used Effect and `@effect/sql-sqlite-wasm` 4.0.0 beta 103 with `@effect/wa-sqlite` 0.1.2. Each operation verified the complete returned byte sequence. OPFS request and response buffers used transfer lists. SQLite insert results compare normal structured clone with `SqliteClient.withTransferables`.

|     Bytes | Path                   | Median ms | p95 ms | Median MiB/s |
| --------: | ---------------------- | --------: | -----: | -----------: |
| 1,048,576 | SQLite clone insert    |     105.5 |  148.0 |          9.5 |
| 1,048,576 | SQLite transfer insert |      82.6 |   92.3 |         12.1 |
| 1,048,576 | SQLite point read      |      73.6 |  105.9 |         13.6 |
| 1,048,576 | OPFS write and flush   |      39.9 |   72.6 |         25.1 |
| 1,048,576 | OPFS point read        |      65.8 |   75.1 |         15.2 |
| 8,388,608 | SQLite clone insert    |     434.5 |  483.3 |         18.4 |
| 8,388,608 | SQLite transfer insert |     348.8 |  460.0 |         22.9 |
| 8,388,608 | SQLite point read      |     143.0 |  180.0 |         55.9 |
| 8,388,608 | OPFS write and flush   |      15.9 |   30.5 |        503.1 |
| 8,388,608 | OPFS point read        |     125.8 |  130.5 |         63.6 |

Seven repetitions were measured per path and size. Browser and filesystem caching affect absolute throughput. The architectural result does not depend on the exact ratio: worker SQLite reads fully materialize every BLOB, copy it out of WASM, then structured clone it to the page. Direct OPFS keeps bytes outside the database, supports independent file lifecycle and ranged reads, and was faster at both measured sizes.

Run locally from the repository root:

```sh
pnpm bench:attachments --host 127.0.0.1 --port 4173 --force
```

Open `http://127.0.0.1:4173/attachment-byte-path.html?repetitions=7&sizes=1048576,8388608` in Chromium. The result is available as `window.__attachmentBenchmark` and rendered in the page.
