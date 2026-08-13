# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4500b7c559212125ae098ac65fc86bb332319dfe`
- Candidate SHA: `cbbeeb46a13883b23cdb77779edc0adf362acd29`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2688956.459 ops/s` | `4962634.512 ops/s` | `+84.56%` | `better` |
| `segment-index-get-live:getMissSync` | `2506286.063 ops/s` | `4410397.713 ops/s` | `+75.97%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1924877.930 ops/s` | `3450966.466 ops/s` | `+79.28%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2590731.345 ops/s` | `4548327.952 ops/s` | `+75.56%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1995050.396 ops/s` | `3358238.801 ops/s` | `+68.33%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2657340.212 ops/s` | `4208394.269 ops/s` | `+58.37%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2406300.538 ops/s` | `3693134.058 ops/s` | `+53.48%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1331650.199 ops/s` | `2011392.790 ops/s` | `+51.05%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `509352.215 ops/s` | `557020.576 ops/s` | `+9.36%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `319757.227 ops/s` | `367580.999 ops/s` | `+14.96%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `189594.988 ops/s` | `189439.577 ops/s` | `-0.08%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `795949.204 ops/s` | `755786.622 ops/s` | `-5.05%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `780776.584 ops/s` | `737098.437 ops/s` | `-5.59%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15172.620 ops/s` | `18688.185 ops/s` | `+23.17%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `3371.719 ops/s` | `7300.535 ops/s` | `+116.52%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3172.692 ops/s` | `6441.470 ops/s` | `+103.03%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1782.382 ops/s` | `3286.977 ops/s` | `+84.41%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1949.830 ops/s` | `3437.103 ops/s` | `+76.28%` | `better` |
| `segment-index-range-scan:boundedScan` | `24.886 us/op` | `31.424 us/op` | `+26.27%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1911.571 us/op` | `2149.381 us/op` | `+12.44%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3565.289 us/op` | `4181.889 us/op` | `+17.29%` | `better` |
| `segment-merge-sequential:mergeSequential` | `307.423 us/op` | `349.544 us/op` | `+13.70%` | `better` |
