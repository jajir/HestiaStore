# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67d6f015595d472d71dbaf369987d5f6eb97b5b5`
- Candidate SHA: `711cbfff12694c500d91ba4d7b0fb9860756a77b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2434625.181 ops/s` | `2961466.063 ops/s` | `+21.64%` | `better` |
| `segment-index-get-live:getMissSync` | `4235514.095 ops/s` | `4822785.415 ops/s` | `+13.87%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7489.633 ops/s` | `8386.321 ops/s` | `+11.97%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3814528.118 ops/s` | `4510281.921 ops/s` | `+18.24%` | `better` |
| `segment-index-get-persisted:getHitSync` | `119572.179 ops/s` | `171947.960 ops/s` | `+43.80%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3751402.345 ops/s` | `5002028.668 ops/s` | `+33.34%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2010777.929 ops/s` | `2595785.918 ops/s` | `+29.09%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1033955.055 ops/s` | `1414552.788 ops/s` | `+36.81%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `283007.487 ops/s` | `339670.182 ops/s` | `+20.02%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `137276.471 ops/s` | `116380.715 ops/s` | `-15.22%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145731.016 ops/s` | `223289.467 ops/s` | `+53.22%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41280.902 ops/s` | `68322.780 ops/s` | `+65.51%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36114.855 ops/s` | `62886.753 ops/s` | `+74.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5166.048 ops/s` | `5436.026 ops/s` | `+5.23%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2459.668 ops/s` | `1479.558 ops/s` | `-39.85%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `440.510 ops/s` | `90.810 ops/s` | `-79.39%` | `worse` |
