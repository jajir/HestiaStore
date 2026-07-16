# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `21d1da156bc01e2f39b9f119c5882d79b3da438a`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2178659.045 ops/s` | `2285478.730 ops/s` | `+4.90%` | `better` |
| `segment-index-get-live:getMissSync` | `1919515.882 ops/s` | `2287707.547 ops/s` | `+19.18%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1608624.422 ops/s` | `1688919.823 ops/s` | `+4.99%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2072513.979 ops/s` | `1942091.302 ops/s` | `-6.29%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2152115.584 ops/s` | `2128432.063 ops/s` | `-1.10%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1041153.217 ops/s` | `1091448.407 ops/s` | `+4.83%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `454070.186 ops/s` | `423773.872 ops/s` | `-6.67%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `302537.421 ops/s` | `252345.588 ops/s` | `-16.59%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `151532.765 ops/s` | `171428.284 ops/s` | `+13.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `465813.882 ops/s` | `534331.973 ops/s` | `+14.71%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `453639.645 ops/s` | `520913.880 ops/s` | `+14.83%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12174.238 ops/s` | `13418.093 ops/s` | `+10.22%` | `better` |
