# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6482912b40c1a3ba60fd56058c0ca8ed4465cc25`
- Candidate SHA: `23eeab2b1995e6d3b9979fd3a979da322a58e9ed`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitAsyncJoin` | `162069.908 ops/s` | `158936.840 ops/s` | `-1.93%` | `neutral` |
| `segment-index-get-live:getHitSync` | `3538320.900 ops/s` | `3444455.971 ops/s` | `-2.65%` | `neutral` |
| `segment-index-get-live:getMissAsyncJoin` | `164748.690 ops/s` | `164204.401 ops/s` | `-0.33%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4211540.151 ops/s` | `3982192.551 ops/s` | `-5.45%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `86.419 ops/s` | `84.706 ops/s` | `-1.98%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `92.268 ops/s` | `87.009 ops/s` | `-5.70%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171051.733 ops/s` | `170889.086 ops/s` | `-0.10%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3777678.556 ops/s` | `3365296.524 ops/s` | `-10.92%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `49005.118 ops/s` | `49403.259 ops/s` | `+0.81%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `114343.903 ops/s` | `110240.699 ops/s` | `-3.59%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `156909.122 ops/s` | `156087.709 ops/s` | `-0.52%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4184579.766 ops/s` | `4049694.118 ops/s` | `-3.22%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2254912.775 ops/s` | `2666492.844 ops/s` | `+18.25%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1552491.306 ops/s` | `1465694.720 ops/s` | `-5.59%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `349015.570 ops/s` | `342713.060 ops/s` | `-1.81%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `180300.579 ops/s` | `162760.665 ops/s` | `-9.73%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168714.991 ops/s` | `179952.395 ops/s` | `+6.66%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `98956.819 ops/s` | `357023.098 ops/s` | `+260.79%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `28969.893 ops/s` | `29796.399 ops/s` | `+2.85%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `69986.926 ops/s` | `327226.699 ops/s` | `+367.55%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2379.018 ops/s` | `2496.181 ops/s` | `+4.92%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2860.458 ops/s` | `2892.486 ops/s` | `+1.12%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2387.478 ops/s` | `2456.934 ops/s` | `+2.91%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2737.346 ops/s` | `2763.604 ops/s` | `+0.96%` | `neutral` |
