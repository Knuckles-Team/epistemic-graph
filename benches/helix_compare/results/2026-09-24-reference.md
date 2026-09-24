### Correctness gate

| workload | engine | passed | checked | failed | recall mean | recall min | samples |
|---|---|---|--:|--:|--:|--:|---|
| point_get | epistemic-graph | yes | 100 | 0 |  |  |  |
| one_hop | epistemic-graph | yes | 100 | 0 |  |  |  |
| filtered_hop | epistemic-graph | yes | 100 | 0 |  |  |  |
| text_prefilter | epistemic-graph | yes | 100 | 0 |  |  |  |
| vector_prefilter | epistemic-graph | yes | 100 | 0 | 0.991 | 0.9 |  |
| mixed | epistemic-graph | NO | 100 | 7 | 0.89 | 0.5 | vector: 8 hits, expected 10; vector: 7 hits, expected 10; vector: 7 hits, expected 10; vector: 9 hits, expected 10; vector: 9 hits, expected 10 |
| txn_batch | epistemic-graph | yes | 100 | 0 |  |  |  |
| point_get | helixdb | yes | 100 | 0 |  |  |  |
| one_hop | helixdb | yes | 100 | 0 |  |  |  |
| filtered_hop | helixdb | yes | 100 | 0 |  |  |  |
| text_prefilter | helixdb | yes | 100 | 0 |  |  |  |
| vector_prefilter | helixdb | yes | 100 | 0 | 1.0 | 1.0 |  |
| mixed | helixdb | yes | 100 | 0 | 1.0 | 1.0 |  |
| txn_batch | helixdb | yes | 100 | 0 |  |  |  |

### Warm latency and throughput

| workload | engine | c | ops | err | p50 ms | p95 ms | p99 ms | ops/s | server CPU ms/op |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| transport_floor | epistemic-graph | 1 | 2000 | 0 | 5.36 | 7.129 | 8.223 | 180.4 | 9.625 |
| transport_floor | epistemic-graph | 8 | 2000 | 0 | 39.314 | 64.206 | 79.174 | 200.2 | 9.28 |
| point_get | epistemic-graph | 1 | 3000 | 0 | 5.682 | 6.751 | 7.97 | 173.6 | 10.067 |
| point_get | epistemic-graph | 8 | 3000 | 0 | 39.594 | 61.559 | 76.895 | 200.1 | 9.407 |
| one_hop | epistemic-graph | 1 | 3000 | 0 | 6.004 | 7.641 | 8.636 | 162.3 | 10.777 |
| one_hop | epistemic-graph | 8 | 3000 | 0 | 43.731 | 71.113 | 85.986 | 181.4 | 10.423 |
| filtered_hop | epistemic-graph | 1 | 2000 | 0 | 6.186 | 8.961 | 9.591 | 153.5 | 11.46 |
| filtered_hop | epistemic-graph | 8 | 2000 | 0 | 44.462 | 66.659 | 79.9 | 176.4 | 10.855 |
| text_prefilter | epistemic-graph | 1 | 1000 | 0 | 449.669 | 547.57 | 722.268 | 2.8 | 708.8 |
| text_prefilter | epistemic-graph | 8 | 1000 | 0 | 1812.258 | 2287.251 | 2637.984 | 4.7 | 631.88 |
| vector_prefilter | epistemic-graph | 1 | 1000 | 0 | 342.386 | 442.319 | 520.465 | 3.4 | 579.81 |
| vector_prefilter | epistemic-graph | 8 | 1000 | 0 | 1243.016 | 1403.95 | 1477.21 | 6.5 | 463.53 |
| transport_floor | helixdb | 1 | 2000 | 0 | 2.427 | 2.979 | 3.555 | 412.4 | 0.14 |
| transport_floor | helixdb | 8 | 2000 | 0 | 16.948 | 37.903 | 45.459 | 355.1 | 0.15 |
| point_get | helixdb | 1 | 3000 | 0 | 4.887 | 6.191 | 6.904 | 204.9 | 2.13 |
| point_get | helixdb | 8 | 3000 | 0 | 24.06 | 62.538 | 98.444 | 258.5 | 1.967 |
| one_hop | helixdb | 1 | 3000 | 0 | 5.552 | 8.621 | 10.4 | 167.0 | 2.933 |
| one_hop | helixdb | 8 | 3000 | 0 | 22.445 | 53.8 | 87.441 | 283.6 | 2.743 |
| filtered_hop | helixdb | 1 | 2000 | 0 | 6.469 | 7.588 | 8.008 | 155.7 | 3.405 |
| filtered_hop | helixdb | 8 | 2000 | 0 | 24.652 | 55.332 | 98.739 | 255.0 | 2.555 |
| text_prefilter | helixdb | 1 | 1000 | 0 | 17.038 | 28.218 | 33.713 | 54.4 | 16.72 |
| text_prefilter | helixdb | 8 | 1000 | 0 | 40.852 | 68.606 | 91.172 | 180.1 | 15.98 |
| vector_prefilter | helixdb | 1 | 1000 | 0 | 46.311 | 68.969 | 86.287 | 21.0 | 45.0 |
| vector_prefilter | helixdb | 8 | 1000 | 0 | 89.727 | 135.55 | 170.194 | 86.3 | 41.67 |
| mixed | helixdb | 1 | 1000 | 0 | 8.063 | 10.738 | 13.073 | 119.4 | 5.4 |
| mixed | helixdb | 8 | 1000 | 0 | 22.952 | 45.108 | 61.7 | 292.1 | 4.6 |
| txn_batch | epistemic-graph | 1 | 150 | 0 | 138.049 | 170.221 | 257.422 | 7.0 | 270.6 |
| txn_batch | epistemic-graph | 8 | 150 | 0 | 1158.032 | 1342.349 | 1392.534 | 6.8 | 285.067 |
| txn_batch | helixdb | 1 | 150 | 0 | 1314.108 | 2934.901 | 3239.208 | 0.6 | 1762.867 |
| txn_batch | helixdb | 8 | 109 | 41 | 8728.164 | 34573.992 | 36826.969 | 0.3 | 10650.0 |

### Process-cold (first queries after restart, c=1)

| workload | engine | c | ops | err | p50 ms | p95 ms | p99 ms | ops/s | server CPU ms/op |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| point_get | epistemic-graph | 1 | 50 | 0 | 8.121 | 30.014 | 799.034 | 37.2 | 50.0 |
| one_hop | epistemic-graph | 1 | 50 | 0 | 8.46 | 12.361 | 12.496 | 114.6 | 16.0 |
| filtered_hop | epistemic-graph | 1 | 50 | 0 | 6.384 | 7.924 | 136.219 | 110.5 | 16.6 |
| text_prefilter | epistemic-graph | 1 | 50 | 0 | 454.144 | 514.385 | 644.877 | 2.2 | 910.8 |
| vector_prefilter | epistemic-graph | 1 | 50 | 0 | 272.099 | 357.841 | 495.242 | 3.5 | 562.6 |
| point_get | helixdb | 1 | 50 | 0 | 3.847 | 4.711 | 8.337 | 252.7 | 10.2 |
| one_hop | helixdb | 1 | 50 | 0 | 4.696 | 5.529 | 5.982 | 208.6 | 5.4 |
| filtered_hop | helixdb | 1 | 50 | 0 | 5.39 | 7.117 | 8.396 | 176.2 | 3.8 |
| text_prefilter | helixdb | 1 | 50 | 0 | 17.683 | 29.196 | 35.413 | 52.7 | 20.4 |
| vector_prefilter | helixdb | 1 | 50 | 0 | 46.233 | 96.423 | 186.353 | 19.2 | 62.6 |
| mixed | helixdb | 1 | 50 | 0 | 8.804 | 10.639 | 11.564 | 112.5 | 6.0 |

### Load and footprint

| engine | load s | docs/s | storage B | amplification | storage B (final) | idle RSS kB | peak RSS kB | write conflict retries | ready empty s | ready after restart s |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| epistemic-graph | 24.91 | 803.0 | 73744384 | 4.706 | 156790784 | 84372 | 1119744 | 0 | 2.98 | 163.18 |
| helixdb | 943.61 | 21.2 | 167866368 | 10.713 | 307986432 | 28704 | 1070544 | 775 | 2.28 | 0.16 |
