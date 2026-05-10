# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `438d4c606becfb314b0d1867aa63f4abdba30751`
- Candidate SHA: `c74955bcb66d3a6d2111d59f658e7170ce7bbcdd`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2327072.768 ops/s` | `1936521.865 ops/s` | `-16.78%` | `worse` |
| `segment-index-get-live:getMissSync` | `3923572.163 ops/s` | `2595227.871 ops/s` | `-33.86%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7053.646 ops/s` | `14289.797 ops/s` | `+102.59%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3644912.954 ops/s` | `2504986.114 ops/s` | `-31.27%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `122761.108 ops/s` | `117826.964 ops/s` | `-4.02%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4026586.080 ops/s` | `2585394.426 ops/s` | `-35.79%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `1928685.071 ops/s` | `1900283.396 ops/s` | `-1.47%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1042717.687 ops/s` | `1077686.546 ops/s` | `+3.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `309122.580 ops/s` | `264060.818 ops/s` | `-14.58%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `139000.099 ops/s` | `89038.051 ops/s` | `-35.94%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170122.480 ops/s` | `175022.767 ops/s` | `+2.88%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `40761.728 ops/s` | `42145.277 ops/s` | `+3.39%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `35508.101 ops/s` | `36858.219 ops/s` | `+3.80%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5253.628 ops/s` | `5287.058 ops/s` | `+0.64%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2704.476 ops/s` | `3065.638 ops/s` | `+13.35%` | `better` |
| `segment-index-persisted-mutation:putSync` | `449.300 ops/s` | `463.525 ops/s` | `+3.17%` | `better` |
