# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `a232ff96e55757742a257a30e43a12f29ad641bd`
- Candidate SHA: `6d8275b140b9735a85ba77e6dd5fd824362c5cc2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1802927.840 ops/s` | `1836378.028 ops/s` | `+1.86%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2308849.375 ops/s` | `2397947.345 ops/s` | `+3.86%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `14246.306 ops/s` | `20434.369 ops/s` | `+43.44%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2237372.944 ops/s` | `2473975.775 ops/s` | `+10.58%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1720456.618 ops/s` | `1772042.618 ops/s` | `+3.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2473911.909 ops/s` | `2385111.848 ops/s` | `-3.59%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1600826.784 ops/s` | `1645620.715 ops/s` | `+2.80%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1071147.773 ops/s` | `1044040.148 ops/s` | `-2.53%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `246069.623 ops/s` | `251891.704 ops/s` | `+2.37%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `99111.386 ops/s` | `88053.108 ops/s` | `-11.16%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `146958.237 ops/s` | `163838.596 ops/s` | `+11.49%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `43383.586 ops/s` | `45330.431 ops/s` | `+4.49%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `38055.225 ops/s` | `40025.311 ops/s` | `+5.18%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5328.362 ops/s` | `5305.120 ops/s` | `-0.44%` | `neutral` |
