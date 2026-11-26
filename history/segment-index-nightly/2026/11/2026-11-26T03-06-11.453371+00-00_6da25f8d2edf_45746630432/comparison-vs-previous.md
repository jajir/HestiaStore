# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3136087.242 ops/s` | `2922133.795 ops/s` | `-6.82%` | `warning` |
| `segment-index-get-live:getMissSync` | `3073885.875 ops/s` | `2158406.148 ops/s` | `-29.78%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `264723.905 ops/s` | `293922.834 ops/s` | `+11.03%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3035590.826 ops/s` | `2847145.084 ops/s` | `-6.21%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `2422366.170 ops/s` | `1843991.825 ops/s` | `-23.88%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3351065.236 ops/s` | `2238054.715 ops/s` | `-33.21%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2370270.636 ops/s` | `1871463.235 ops/s` | `-21.04%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3311776.869 ops/s` | `2254029.793 ops/s` | `-31.94%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2596061.579 ops/s` | `2173508.132 ops/s` | `-16.28%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1416649.354 ops/s` | `1173140.341 ops/s` | `-17.19%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `122.341 ms/op` | `127.003 ms/op` | `+3.81%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `138.662 ms/op` | `142.529 ms/op` | `+2.79%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `120.507 ms/op` | `121.844 ms/op` | `+1.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `468318.623 ops/s` | `473639.079 ops/s` | `+1.14%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `210219.307 ops/s` | `208056.279 ops/s` | `-1.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `258099.316 ops/s` | `265582.800 ops/s` | `+2.90%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `989856.373 ops/s` | `849986.694 ops/s` | `-14.13%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `956003.064 ops/s` | `810983.517 ops/s` | `-15.17%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `33853.309 ops/s` | `39003.177 ops/s` | `+15.21%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `10777.426 ops/s` | `5298.193 ops/s` | `-50.84%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `10546.879 ops/s` | `5466.166 ops/s` | `-48.17%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `4339.247 ops/s` | `2274.698 ops/s` | `-47.58%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `4085.435 ops/s` | `2333.135 ops/s` | `-42.89%` | `worse` |
| `segment-index-range-scan:boundedScan` | `25.109 us/op` | `24.013 us/op` | `-4.36%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2059.783 us/op` | `1843.873 us/op` | `-10.48%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3933.678 us/op` | `3532.988 us/op` | `-10.19%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `337.774 us/op` | `316.850 us/op` | `-6.19%` | `warning` |
