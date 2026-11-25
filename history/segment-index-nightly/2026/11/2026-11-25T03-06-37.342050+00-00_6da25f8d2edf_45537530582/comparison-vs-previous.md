# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6467945.393 ops/s` | `3136087.242 ops/s` | `-51.51%` | `worse` |
| `segment-index-get-live:getMissSync` | `5911305.698 ops/s` | `3073885.875 ops/s` | `-48.00%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `482060.631 ops/s` | `264723.905 ops/s` | `-45.08%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `6404477.378 ops/s` | `3035590.826 ops/s` | `-52.60%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5373193.501 ops/s` | `2422366.170 ops/s` | `-54.92%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5986682.646 ops/s` | `3351065.236 ops/s` | `-44.02%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `5568704.060 ops/s` | `2370270.636 ops/s` | `-57.44%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `6457584.214 ops/s` | `3311776.869 ops/s` | `-48.71%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5867022.484 ops/s` | `2596061.579 ops/s` | `-55.75%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3191908.164 ops/s` | `1416649.354 ops/s` | `-55.62%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `195.782 ms/op` | `122.341 ms/op` | `-37.51%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `206.731 ms/op` | `138.662 ms/op` | `-32.93%` | `worse` |
| `segment-index-lifecycle:openExisting` | `191.388 ms/op` | `120.507 ms/op` | `-37.04%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `738533.196 ops/s` | `468318.623 ops/s` | `-36.59%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `446727.410 ops/s` | `210219.307 ops/s` | `-52.94%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `291805.786 ops/s` | `258099.316 ops/s` | `-11.55%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `2165652.775 ops/s` | `989856.373 ops/s` | `-54.29%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `2116402.907 ops/s` | `956003.064 ops/s` | `-54.83%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `49249.868 ops/s` | `33853.309 ops/s` | `-31.26%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6909.499 ops/s` | `10777.426 ops/s` | `+55.98%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7579.400 ops/s` | `10546.879 ops/s` | `+39.15%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3483.579 ops/s` | `4339.247 ops/s` | `+24.56%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3244.462 ops/s` | `4085.435 ops/s` | `+25.92%` | `better` |
| `segment-index-range-scan:boundedScan` | `18.757 us/op` | `25.109 us/op` | `+33.86%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1190.919 us/op` | `2059.783 us/op` | `+72.96%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2356.659 us/op` | `3933.678 us/op` | `+66.92%` | `better` |
| `segment-merge-sequential:mergeSequential` | `181.763 us/op` | `337.774 us/op` | `+85.83%` | `better` |
