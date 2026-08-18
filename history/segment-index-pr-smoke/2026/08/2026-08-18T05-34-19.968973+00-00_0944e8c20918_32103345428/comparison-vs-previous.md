# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `cbbeeb46a13883b23cdb77779edc0adf362acd29`
- Candidate SHA: `0944e8c20918f769044f88a7c62655cf623defed`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4962634.512 ops/s` | `6076580.097 ops/s` | `+22.45%` | `better` |
| `segment-index-get-live:getMissSync` | `4410397.713 ops/s` | `5843703.007 ops/s` | `+32.50%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3450966.466 ops/s` | `4556868.535 ops/s` | `+32.05%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4548327.952 ops/s` | `5458178.632 ops/s` | `+20.00%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3358238.801 ops/s` | `4317460.049 ops/s` | `+28.56%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4208394.269 ops/s` | `5920451.977 ops/s` | `+40.68%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3693134.058 ops/s` | `4941760.155 ops/s` | `+33.81%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2011392.790 ops/s` | `2792947.094 ops/s` | `+38.86%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `557020.576 ops/s` | `657540.717 ops/s` | `+18.05%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `367580.999 ops/s` | `404359.867 ops/s` | `+10.01%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `189439.577 ops/s` | `253180.850 ops/s` | `+33.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `755786.622 ops/s` | `1482927.237 ops/s` | `+96.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `737098.437 ops/s` | `1458310.424 ops/s` | `+97.84%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18688.185 ops/s` | `24616.813 ops/s` | `+31.72%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7300.535 ops/s` | `3462.535 ops/s` | `-52.57%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6441.470 ops/s` | `3523.490 ops/s` | `-45.30%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3286.977 ops/s` | `1994.828 ops/s` | `-39.31%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3437.103 ops/s` | `1942.867 ops/s` | `-43.47%` | `worse` |
| `segment-index-range-scan:boundedScan` | `31.424 us/op` | `23.251 us/op` | `-26.01%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2149.381 us/op` | `1626.288 us/op` | `-24.34%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4181.889 us/op` | `3180.528 us/op` | `-23.95%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `349.544 us/op` | `273.537 us/op` | `-21.74%` | `worse` |
