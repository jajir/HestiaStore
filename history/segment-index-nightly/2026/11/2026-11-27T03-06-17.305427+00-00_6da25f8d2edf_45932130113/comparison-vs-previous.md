# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2922133.795 ops/s` | `6617236.941 ops/s` | `+126.45%` | `better` |
| `segment-index-get-live:getMissSync` | `2158406.148 ops/s` | `6952214.681 ops/s` | `+222.10%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `293922.834 ops/s` | `473551.721 ops/s` | `+61.11%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2847145.084 ops/s` | `6650979.343 ops/s` | `+133.60%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1843991.825 ops/s` | `5635503.132 ops/s` | `+205.61%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2238054.715 ops/s` | `5972192.582 ops/s` | `+166.85%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1871463.235 ops/s` | `4657792.957 ops/s` | `+148.89%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2254029.793 ops/s` | `6712350.590 ops/s` | `+197.79%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2173508.132 ops/s` | `6267664.385 ops/s` | `+188.37%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1173140.341 ops/s` | `3180008.224 ops/s` | `+171.07%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `127.003 ms/op` | `202.845 ms/op` | `+59.72%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `142.529 ms/op` | `210.378 ms/op` | `+47.60%` | `better` |
| `segment-index-lifecycle:openExisting` | `121.844 ms/op` | `201.100 ms/op` | `+65.05%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `473639.079 ops/s` | `735343.389 ops/s` | `+55.25%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `208056.279 ops/s` | `448016.361 ops/s` | `+115.33%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `265582.800 ops/s` | `287327.028 ops/s` | `+8.19%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `849986.694 ops/s` | `1920407.115 ops/s` | `+125.93%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `810983.517 ops/s` | `1869529.368 ops/s` | `+130.53%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `39003.177 ops/s` | `50877.747 ops/s` | `+30.45%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5298.193 ops/s` | `5486.989 ops/s` | `+3.56%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5466.166 ops/s` | `5684.213 ops/s` | `+3.99%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2274.698 ops/s` | `3268.726 ops/s` | `+43.70%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2333.135 ops/s` | `2897.999 ops/s` | `+24.21%` | `better` |
| `segment-index-range-scan:boundedScan` | `24.013 us/op` | `17.101 us/op` | `-28.79%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1843.873 us/op` | `1157.448 us/op` | `-37.23%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3532.988 us/op` | `2263.074 us/op` | `-35.94%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `316.850 us/op` | `175.718 us/op` | `-44.54%` | `worse` |
