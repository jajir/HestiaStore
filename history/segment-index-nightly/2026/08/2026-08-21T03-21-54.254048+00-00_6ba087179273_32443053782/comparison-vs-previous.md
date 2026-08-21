# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4256750.029 ops/s` | `5212664.973 ops/s` | `+22.46%` | `better` |
| `segment-index-get-live:getMissSync` | `4428066.556 ops/s` | `4579751.284 ops/s` | `+3.43%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `285340.813 ops/s` | `273969.747 ops/s` | `-3.99%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4255511.935 ops/s` | `4441262.091 ops/s` | `+4.36%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3526148.200 ops/s` | `3610869.443 ops/s` | `+2.40%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3957283.144 ops/s` | `4011685.205 ops/s` | `+1.37%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3424899.241 ops/s` | `3642368.526 ops/s` | `+6.35%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4539923.485 ops/s` | `4375237.968 ops/s` | `-3.63%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4056473.857 ops/s` | `4369900.391 ops/s` | `+7.73%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2119230.623 ops/s` | `2270937.419 ops/s` | `+7.16%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `276.381 ms/op` | `244.757 ms/op` | `-11.44%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `299.359 ms/op` | `268.280 ms/op` | `-10.38%` | `worse` |
| `segment-index-lifecycle:openExisting` | `271.413 ms/op` | `243.825 ms/op` | `-10.16%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `544853.637 ops/s` | `525134.359 ops/s` | `-3.62%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `262989.613 ops/s` | `253684.369 ops/s` | `-3.54%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `281864.024 ops/s` | `271449.990 ops/s` | `-3.69%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1261037.940 ops/s` | `1264818.010 ops/s` | `+0.30%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1239694.790 ops/s` | `1244251.951 ops/s` | `+0.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `21343.151 ops/s` | `20566.059 ops/s` | `-3.64%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8290.534 ops/s` | `6605.569 ops/s` | `-20.32%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8185.416 ops/s` | `6791.968 ops/s` | `-17.02%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3255.188 ops/s` | `2326.839 ops/s` | `-28.52%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3159.621 ops/s` | `2187.610 ops/s` | `-30.76%` | `worse` |
| `segment-index-range-scan:boundedScan` | `31.053 us/op` | `27.455 us/op` | `-11.59%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2128.254 us/op` | `2023.157 us/op` | `-4.94%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4205.713 us/op` | `4083.088 us/op` | `-2.92%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `358.481 us/op` | `332.259 us/op` | `-7.31%` | `worse` |
