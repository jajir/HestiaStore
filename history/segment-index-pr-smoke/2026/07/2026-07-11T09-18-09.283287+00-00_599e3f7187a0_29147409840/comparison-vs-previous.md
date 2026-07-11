# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c4e36341d5090cbedaf4272dfe8a9d7ca1569f19`
- Candidate SHA: `599e3f7187a0fa5414ce9686d524d8b89fc6671d`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2284467.295 ops/s` | `2338274.547 ops/s` | `+2.36%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2053751.138 ops/s` | `1918932.023 ops/s` | `-6.56%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1874645.735 ops/s` | `1992828.535 ops/s` | `+6.30%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2145457.934 ops/s` | `2185867.605 ops/s` | `+1.88%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2118349.097 ops/s` | `2175750.150 ops/s` | `+2.71%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1105218.983 ops/s` | `1106908.385 ops/s` | `+0.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `449421.779 ops/s` | `463205.226 ops/s` | `+3.07%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `283063.429 ops/s` | `297922.263 ops/s` | `+5.25%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `166358.350 ops/s` | `165282.963 ops/s` | `-0.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `628151.880 ops/s` | `652752.312 ops/s` | `+3.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `613638.778 ops/s` | `637098.080 ops/s` | `+3.82%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14513.102 ops/s` | `15654.231 ops/s` | `+7.86%` | `better` |
