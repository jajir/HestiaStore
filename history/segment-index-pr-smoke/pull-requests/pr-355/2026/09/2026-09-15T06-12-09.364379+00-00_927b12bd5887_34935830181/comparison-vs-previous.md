# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `927b12bd5887e6bdcfc45404e71b5cfe8522acc2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `6539238.920 ops/s` | `+38.93%` | `better` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `6453153.256 ops/s` | `+43.53%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `5226847.191 ops/s` | `+49.10%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `6380298.450 ops/s` | `+42.81%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `5588314.470 ops/s` | `+73.72%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `6739419.654 ops/s` | `+56.71%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `6181683.725 ops/s` | `+57.97%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `3340931.103 ops/s` | `+64.76%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `753275.171 ops/s` | `+34.69%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `466978.860 ops/s` | `+23.47%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `286296.311 ops/s` | `+58.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `1499118.562 ops/s` | `+91.29%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `1454871.585 ops/s` | `+90.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `44246.977 ops/s` | `+135.07%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `2077.482 ops/s` | `-73.24%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `2884.142 ops/s` | `-62.17%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `427.041 ops/s` | `-88.05%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `590.614 ops/s` | `-83.38%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `18.346 us/op` | `-47.44%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `1164.371 us/op` | `-45.62%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `2261.080 us/op` | `-47.78%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `174.331 us/op` | `-50.05%` | `worse` |
