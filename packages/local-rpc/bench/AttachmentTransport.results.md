# Attachment transport byte path benchmark

Environment: Node v24.15.0, Darwin arm64, Apple Silicon host.

Method: 1 fixed warmup and 3 measured repetitions per case. Payloads use deterministic bytes in 256 KiB chunks. Each operation uploads and downloads the complete payload, then compares every returned chunk. Concurrency is either sequential or bounded at 4. Memory peaks are sampled at every application or provider chunk boundary through `process.memoryUsage()`. Deltas compare the last sample with the pre operation sample. Application byte counters include attachment payload only. Grant metadata is excluded.

The benchmark configured 64 MiB as the maximum attachment size. The application proxy fixture retains the production HEAD, PATCH, and GET streaming shape so this baseline remains measurable after the production proxy routes are removed.

## Aggregate

| Path              | Size MiB | Concurrency | Median ms | p95 ms | Median MiB/s | Heap peak delta MiB | RSS peak delta MiB | App ingress MiB | App egress MiB | Provider MiB |
| ----------------- | -------: | ----------: | --------: | -----: | -----------: | ------------------: | -----------------: | --------------: | -------------: | -----------: |
| application proxy |        1 |           1 |      2.07 |   4.82 |       963.89 |                0.66 |               4.14 |            1.00 |           1.00 |         2.00 |
| direct provider   |        1 |           1 |      0.89 |   1.05 |      2257.02 |                0.13 |               4.13 |            0.00 |           0.00 |         2.00 |
| application proxy |        1 |           4 |      6.61 |  21.62 |      1210.01 |                1.18 |              13.27 |            4.00 |           4.00 |         8.00 |
| direct provider   |        1 |           4 |      5.03 |  13.88 |      1589.15 |                0.50 |              16.00 |            0.00 |           0.00 |         8.00 |
| application proxy |        8 |           1 |      5.92 |  10.82 |      2701.33 |                0.70 |               8.97 |            8.00 |           8.00 |        16.00 |
| direct provider   |        8 |           1 |      7.10 |   7.64 |      2253.60 |                0.21 |              31.86 |            0.00 |           0.00 |        16.00 |
| application proxy |        8 |           4 |     33.48 |  49.45 |      1911.54 |                1.00 |              48.47 |           32.00 |          32.00 |        64.00 |
| direct provider   |        8 |           4 |     28.01 |  28.60 |      2285.29 |                0.32 |              29.55 |            0.00 |           0.00 |        64.00 |
| application proxy |       64 |           1 |     53.25 |  61.44 |      2403.82 |                0.58 |              13.75 |           64.00 |          64.00 |       128.00 |
| direct provider   |       64 |           1 |     52.46 | 181.06 |      2440.17 |                0.27 |              38.77 |            0.00 |           0.00 |       128.00 |
| application proxy |       64 |           4 |    218.19 | 229.40 |      2346.56 |                1.29 |              23.30 |          256.00 |         256.00 |       512.00 |
| direct provider   |       64 |           4 |    195.68 | 274.98 |      2616.47 |                0.60 |              17.53 |            0.00 |           0.00 |       512.00 |

The small sample timing and RSS variance show that this is a directional local fixture, not a capacity forecast. The stable architectural result is the byte accounting. Proxy transfers send one full upload and one full download through the application origin. Direct provider transfers send zero attachment payload bytes through it.

## Raw samples

Heap and RSS values are MiB. Negative end deltas mean garbage collection reclaimed memory during a sample.

| Path              | Size MiB | Concurrency | Sample |     ms |   MiB/s | Heap end delta | Heap peak delta | RSS end delta | RSS peak delta |
| ----------------- | -------: | ----------: | -----: | -----: | ------: | -------------: | --------------: | ------------: | -------------: |
| application proxy |        1 |           1 |      1 |   2.07 |  963.89 |           0.43 |            0.66 |          4.14 |           4.14 |
| application proxy |        1 |           1 |      2 |   1.67 | 1196.38 |           0.67 |            0.86 |          4.17 |           4.17 |
| application proxy |        1 |           1 |      3 |   4.82 |  415.31 |           0.16 |            0.16 |          2.91 |           2.91 |
| direct provider   |        1 |           1 |      1 |   1.05 | 1913.11 |           0.13 |            0.13 |          4.13 |           4.13 |
| direct provider   |        1 |           1 |      2 |   0.89 | 2257.02 |           0.14 |            0.20 |          4.13 |           4.13 |
| direct provider   |        1 |           1 |      3 |   0.83 | 2402.04 |           0.13 |            0.13 |          4.13 |           4.13 |
| application proxy |        1 |           4 |      1 |   4.55 | 1758.02 |           0.99 |            1.22 |         17.14 |          17.14 |
| application proxy |        1 |           4 |      2 |  21.62 |  370.02 |          -3.28 |            1.02 |         10.39 |          10.39 |
| application proxy |        1 |           4 |      3 |   6.61 | 1210.01 |           1.18 |            1.18 |         13.27 |          13.27 |
| direct provider   |        1 |           4 |      1 |   3.54 | 2257.10 |           0.24 |            0.69 |         16.17 |          16.17 |
| direct provider   |        1 |           4 |      2 |   5.03 | 1589.15 |           0.50 |            0.50 |         16.00 |          16.00 |
| direct provider   |        1 |           4 |      3 |  13.88 |  576.50 |          -2.64 |            0.33 |          6.80 |           6.80 |
| application proxy |        8 |           1 |      1 |   5.59 | 2860.02 |           0.59 |            0.89 |         21.53 |          21.53 |
| application proxy |        8 |           1 |      2 |  10.82 | 1479.12 |          -1.02 |            0.45 |          8.97 |           8.97 |
| application proxy |        8 |           1 |      3 |   5.92 | 2701.33 |           0.59 |            0.70 |          8.81 |           8.81 |
| direct provider   |        8 |           1 |      1 |   7.10 | 2253.60 |           0.21 |            0.21 |         13.56 |          13.56 |
| direct provider   |        8 |           1 |      2 |   7.64 | 2095.10 |          -1.98 |            0.10 |         31.86 |          31.86 |
| direct provider   |        8 |           1 |      3 |   4.37 | 3662.10 |           0.21 |            0.21 |         32.00 |          32.00 |
| application proxy |        8 |           4 |      1 |  33.48 | 1911.54 |          -0.09 |            1.00 |         38.39 |          38.39 |
| application proxy |        8 |           4 |      2 |  27.65 | 2314.28 |          -0.05 |            1.24 |         99.45 |          99.45 |
| application proxy |        8 |           4 |      3 |  49.45 | 1294.26 |           0.49 |            0.67 |         48.47 |          48.47 |
| direct provider   |        8 |           4 |      1 |  25.42 | 2518.10 |          -0.06 |            0.30 |          2.63 |           2.63 |
| direct provider   |        8 |           4 |      2 |  28.01 | 2285.29 |           0.00 |            0.32 |         32.58 |          32.58 |
| direct provider   |        8 |           4 |      3 |  28.60 | 2237.53 |           0.05 |            0.33 |         29.55 |          29.55 |
| application proxy |       64 |           1 |      1 |  53.25 | 2403.82 |           0.58 |            0.58 |         21.66 |          21.66 |
| application proxy |       64 |           1 |      2 |  61.44 | 2083.39 |          -0.81 |            0.18 |         13.75 |          13.75 |
| application proxy |       64 |           1 |      3 |  50.01 | 2559.65 |          -0.02 |            0.75 |          0.16 |           0.16 |
| direct provider   |       64 |           1 |      1 | 181.06 |  706.95 |          -0.03 |            0.28 |         57.50 |          57.50 |
| direct provider   |       64 |           1 |      2 |  52.46 | 2440.17 |           0.09 |            0.19 |         11.25 |          11.25 |
| direct provider   |       64 |           1 |      3 |  49.86 | 2567.25 |           0.08 |            0.27 |         38.77 |          38.77 |
| application proxy |       64 |           4 |      1 | 218.19 | 2346.56 |          -0.77 |            0.42 |          6.45 |           6.45 |
| application proxy |       64 |           4 |      2 | 204.70 | 2501.22 |           0.64 |            1.57 |         53.83 |          53.83 |
| application proxy |       64 |           4 |      3 | 229.40 | 2231.94 |           1.20 |            1.29 |         23.30 |          23.30 |
| direct provider   |       64 |           4 |      1 | 274.98 | 1861.98 |          -0.10 |            0.60 |         17.53 |          17.53 |
| direct provider   |       64 |           4 |      2 | 195.68 | 2616.47 |           0.18 |            0.79 |         23.75 |          23.75 |
| direct provider   |       64 |           4 |      3 | 182.16 | 2810.69 |          -0.05 |            0.37 |         13.38 |          13.38 |
