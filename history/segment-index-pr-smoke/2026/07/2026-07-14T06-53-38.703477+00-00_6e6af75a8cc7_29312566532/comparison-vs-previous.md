# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `599e3f7187a0fa5414ce9686d524d8b89fc6671d`
- Candidate SHA: `6e6af75a8cc72508969ed93c52913c0d8b2a8916`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2105911.234 ops/s` | `2253600.397 ops/s` | `+7.01%` | `better` |
| `segment-index-get-live:getMissSync` | `2113374.027 ops/s` | `2113150.233 ops/s` | `-0.01%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2137951.007 ops/s` | `1696965.342 ops/s` | `-20.63%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2070652.875 ops/s` | `2116830.311 ops/s` | `+2.23%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2186537.171 ops/s` | `2236256.144 ops/s` | `+2.27%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1090953.638 ops/s` | `1129240.120 ops/s` | `+3.51%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `469440.531 ops/s` | `437534.290 ops/s` | `-6.80%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `295627.451 ops/s` | `262261.116 ops/s` | `-11.29%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `173813.080 ops/s` | `175273.174 ops/s` | `+0.84%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `695036.776 ops/s` | `661059.444 ops/s` | `-4.89%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `678398.112 ops/s` | `645495.399 ops/s` | `-4.85%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16638.664 ops/s` | `15564.045 ops/s` | `-6.46%` | `warning` |
