# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `042a54fc5b39a821f487e959141a4dd5b63ff8e7`
- Candidate SHA: `8ed247d12bbab6bb585f9ca2ece5cf2213da2ac2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2310249.135 ops/s` | `2235190.350 ops/s` | `-3.25%` | `warning` |
| `segment-index-get-live:getMissSync` | `3780815.866 ops/s` | `3834281.093 ops/s` | `+1.41%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `6791.397 ops/s` | `6983.907 ops/s` | `+2.83%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3671750.889 ops/s` | `3829852.562 ops/s` | `+4.31%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1887399.488 ops/s` | `1658940.010 ops/s` | `-12.10%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4044717.574 ops/s` | `3890648.059 ops/s` | `-3.81%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1923139.002 ops/s` | `1860657.022 ops/s` | `-3.25%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1117855.857 ops/s` | `1104716.674 ops/s` | `-1.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `274188.353 ops/s` | `285076.150 ops/s` | `+3.97%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `117469.039 ops/s` | `132798.017 ops/s` | `+13.05%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `156719.314 ops/s` | `152278.134 ops/s` | `-2.83%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `36382.004 ops/s` | `41949.448 ops/s` | `+15.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `31136.754 ops/s` | `36775.964 ops/s` | `+18.11%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5245.250 ops/s` | `5173.484 ops/s` | `-1.37%` | `neutral` |
