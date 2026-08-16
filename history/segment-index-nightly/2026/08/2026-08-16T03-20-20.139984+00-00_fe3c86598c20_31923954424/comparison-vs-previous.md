# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4888469.468 ops/s` | `4503847.645 ops/s` | `-7.87%` | `worse` |
| `segment-index-get-live:getMissSync` | `4750463.358 ops/s` | `4765500.987 ops/s` | `+0.32%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `269822.513 ops/s` | `279684.970 ops/s` | `+3.66%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4195918.973 ops/s` | `4303097.162 ops/s` | `+2.55%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3485166.553 ops/s` | `3631514.856 ops/s` | `+4.20%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4968381.643 ops/s` | `4503318.619 ops/s` | `-9.36%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3562768.155 ops/s` | `3395544.249 ops/s` | `-4.69%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4980025.993 ops/s` | `4508951.257 ops/s` | `-9.46%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `3787075.814 ops/s` | `4503864.514 ops/s` | `+18.93%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2361983.337 ops/s` | `2246386.558 ops/s` | `-4.89%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.542 ms/op` | `247.818 ms/op` | `+0.93%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `269.784 ms/op` | `268.085 ms/op` | `-0.63%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `242.180 ms/op` | `242.069 ms/op` | `-0.05%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `506562.641 ops/s` | `528578.205 ops/s` | `+4.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `272584.962 ops/s` | `277192.340 ops/s` | `+1.69%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `233977.679 ops/s` | `251385.864 ops/s` | `+7.44%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1180775.554 ops/s` | `1132218.687 ops/s` | `-4.11%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1166690.773 ops/s` | `1117740.211 ops/s` | `-4.20%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14084.781 ops/s` | `14478.476 ops/s` | `+2.80%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6366.132 ops/s` | `7473.543 ops/s` | `+17.40%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6762.014 ops/s` | `7425.606 ops/s` | `+9.81%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2145.162 ops/s` | `2507.280 ops/s` | `+16.88%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2109.875 ops/s` | `2432.141 ops/s` | `+15.27%` | `better` |
| `segment-index-range-scan:boundedScan` | `26.797 us/op` | `32.529 us/op` | `+21.39%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2065.433 us/op` | `2038.984 us/op` | `-1.28%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4136.364 us/op` | `4048.810 us/op` | `-2.12%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `333.582 us/op` | `333.018 us/op` | `-0.17%` | `neutral` |
