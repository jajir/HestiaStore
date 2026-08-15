# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2987874.916 ops/s` | `4888469.468 ops/s` | `+63.61%` | `better` |
| `segment-index-get-live:getMissSync` | `2417427.065 ops/s` | `4750463.358 ops/s` | `+96.51%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `302680.119 ops/s` | `269822.513 ops/s` | `-10.86%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2682725.966 ops/s` | `4195918.973 ops/s` | `+56.41%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1987969.037 ops/s` | `3485166.553 ops/s` | `+75.31%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2732249.075 ops/s` | `4968381.643 ops/s` | `+81.84%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1789822.114 ops/s` | `3562768.155 ops/s` | `+99.06%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2729407.885 ops/s` | `4980025.993 ops/s` | `+82.46%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2524611.653 ops/s` | `3787075.814 ops/s` | `+50.01%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1576181.545 ops/s` | `2361983.337 ops/s` | `+49.85%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `135.793 ms/op` | `245.542 ms/op` | `+80.82%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `157.562 ms/op` | `269.784 ms/op` | `+71.22%` | `better` |
| `segment-index-lifecycle:openExisting` | `132.629 ms/op` | `242.180 ms/op` | `+82.60%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `468082.498 ops/s` | `506562.641 ops/s` | `+8.22%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `214728.595 ops/s` | `272584.962 ops/s` | `+26.94%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `253353.903 ops/s` | `233977.679 ops/s` | `-7.65%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1131388.355 ops/s` | `1180775.554 ops/s` | `+4.37%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1113984.939 ops/s` | `1166690.773 ops/s` | `+4.73%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17403.416 ops/s` | `14084.781 ops/s` | `-19.07%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `4948.739 ops/s` | `6366.132 ops/s` | `+28.64%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `4779.397 ops/s` | `6762.014 ops/s` | `+41.48%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2123.185 ops/s` | `2145.162 ops/s` | `+1.04%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2073.446 ops/s` | `2109.875 ops/s` | `+1.76%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `24.508 us/op` | `26.797 us/op` | `+9.34%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1806.837 us/op` | `2065.433 us/op` | `+14.31%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3554.100 us/op` | `4136.364 us/op` | `+16.38%` | `better` |
| `segment-merge-sequential:mergeSequential` | `304.015 us/op` | `333.582 us/op` | `+9.73%` | `better` |
