# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `aedd79c976d8d4509d34174b33b15c6a18a0409a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `6266428.431 ops/s` | `+33.13%` | `better` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `5477869.584 ops/s` | `+21.84%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `4551650.269 ops/s` | `+29.84%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `5650178.855 ops/s` | `+26.47%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `4462625.172 ops/s` | `+38.72%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `6205707.582 ops/s` | `+44.30%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `5211907.265 ops/s` | `+33.19%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `2587135.581 ops/s` | `+27.59%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `700562.039 ops/s` | `+25.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `420785.229 ops/s` | `+11.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `279776.811 ops/s` | `+54.53%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `1227744.386 ops/s` | `+56.66%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `1188035.037 ops/s` | `+55.33%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `39709.348 ops/s` | `+110.97%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `5999.451 ops/s` | `-22.71%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `6040.592 ops/s` | `-20.77%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `2776.197 ops/s` | `-22.30%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `639.739 ops/s` | `-81.99%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `29.921 us/op` | `-14.27%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `1592.124 us/op` | `-25.64%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `3069.438 us/op` | `-29.11%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `272.069 us/op` | `-22.05%` | `worse` |
