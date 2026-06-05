# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52d497836094b563d34e36a07610249242fa9d7b`
- Candidate SHA: `1d013e75d1cb1d4624c28c09ef20598b553fcfb9`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2193649.088 ops/s` | `2331216.309 ops/s` | `+6.27%` | `better` |
| `segment-index-get-live:getMissSync` | `1881820.157 ops/s` | `2008312.117 ops/s` | `+6.72%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1887247.170 ops/s` | `1867429.533 ops/s` | `-1.05%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2017417.450 ops/s` | `2046184.917 ops/s` | `+1.43%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2078632.935 ops/s` | `2078176.222 ops/s` | `-0.02%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1114794.087 ops/s` | `1063029.013 ops/s` | `-4.64%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `303540.855 ops/s` | `298300.237 ops/s` | `-1.73%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `132762.608 ops/s` | `142088.208 ops/s` | `+7.02%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170778.247 ops/s` | `156212.030 ops/s` | `-8.53%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `181460.240 ops/s` | `166315.503 ops/s` | `-8.35%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `167395.727 ops/s` | `152448.843 ops/s` | `-8.93%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14064.513 ops/s` | `13866.661 ops/s` | `-1.41%` | `neutral` |
