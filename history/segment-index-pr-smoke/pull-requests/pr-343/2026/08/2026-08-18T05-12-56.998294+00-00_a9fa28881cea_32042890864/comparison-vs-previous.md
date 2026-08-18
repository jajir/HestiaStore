# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `cbbeeb46a13883b23cdb77779edc0adf362acd29`
- Candidate SHA: `a9fa28881cea0239bf7461ec50125855350799ea`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4962634.512 ops/s` | `4890027.425 ops/s` | `-1.46%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4410397.713 ops/s` | `4479908.985 ops/s` | `+1.58%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3450966.466 ops/s` | `3216692.106 ops/s` | `-6.79%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4548327.952 ops/s` | `4430779.005 ops/s` | `-2.58%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3358238.801 ops/s` | `3438224.082 ops/s` | `+2.38%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4208394.269 ops/s` | `4518312.659 ops/s` | `+7.36%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3693134.058 ops/s` | `3961568.388 ops/s` | `+7.27%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2011392.790 ops/s` | `2111000.897 ops/s` | `+4.95%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `557020.576 ops/s` | `517817.113 ops/s` | `-7.04%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `367580.999 ops/s` | `323886.901 ops/s` | `-11.89%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `189439.577 ops/s` | `193930.212 ops/s` | `+2.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `755786.622 ops/s` | `891517.636 ops/s` | `+17.96%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `737098.437 ops/s` | `871839.380 ops/s` | `+18.28%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18688.185 ops/s` | `19678.256 ops/s` | `+5.30%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7300.535 ops/s` | `7536.685 ops/s` | `+3.23%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6441.470 ops/s` | `7751.019 ops/s` | `+20.33%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3286.977 ops/s` | `3316.057 ops/s` | `+0.88%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3437.103 ops/s` | `3559.204 ops/s` | `+3.55%` | `better` |
| `segment-index-range-scan:boundedScan` | `31.424 us/op` | `35.950 us/op` | `+14.40%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2149.381 us/op` | `2162.543 us/op` | `+0.61%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4181.889 us/op` | `4245.613 us/op` | `+1.52%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `349.544 us/op` | `349.081 us/op` | `-0.13%` | `neutral` |
