# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `3af5226638894ad29c7a620a80e11f651393a855`
- Candidate SHA: `6309468d3d7f8677a76ad087d9c50bde66b3f144`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2461676.236 ops/s` | `2427106.237 ops/s` | `-1.40%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2017824.379 ops/s` | `2442151.733 ops/s` | `+21.03%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1835901.142 ops/s` | `1878391.751 ops/s` | `+2.31%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2399840.916 ops/s` | `2226355.925 ops/s` | `-7.23%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2085441.874 ops/s` | `2178865.201 ops/s` | `+4.48%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1080777.337 ops/s` | `1101946.208 ops/s` | `+1.96%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `449192.489 ops/s` | `429097.456 ops/s` | `-4.47%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `286383.800 ops/s` | `259910.257 ops/s` | `-9.24%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162808.690 ops/s` | `169187.199 ops/s` | `+3.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `543242.078 ops/s` | `629614.775 ops/s` | `+15.90%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `529314.943 ops/s` | `614686.161 ops/s` | `+16.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13927.136 ops/s` | `14928.614 ops/s` | `+7.19%` | `better` |
