# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9c725bd833aba4b408c00a1771c1bb051d786844`
- Candidate SHA: `2f4865fdc9ecad9ed60b5103c409bbc35b61b94e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2185823.015 ops/s` | `2044040.126 ops/s` | `-6.49%` | `warning` |
| `segment-index-get-live:getMissSync` | `4319447.344 ops/s` | `3926407.567 ops/s` | `-9.10%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `82.347 ops/s` | `84.110 ops/s` | `+2.14%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4136793.584 ops/s` | `4078800.649 ops/s` | `-1.40%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115300.944 ops/s` | `119481.349 ops/s` | `+3.63%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4309181.466 ops/s` | `3609686.367 ops/s` | `-16.23%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2168026.691 ops/s` | `2285046.521 ops/s` | `+5.40%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1151292.599 ops/s` | `1125961.689 ops/s` | `-2.20%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `294210.115 ops/s` | `341677.058 ops/s` | `+16.13%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `132097.226 ops/s` | `163347.697 ops/s` | `+23.66%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162112.889 ops/s` | `178329.361 ops/s` | `+10.00%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `16683.514 ops/s` | `3734.695 ops/s` | `-77.61%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `3793.865 ops/s` | `2901.322 ops/s` | `-23.53%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12889.649 ops/s` | `833.373 ops/s` | `-93.53%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2121.992 ops/s` | `2098.921 ops/s` | `-1.09%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `1985.697 ops/s` | `2068.767 ops/s` | `+4.18%` | `better` |
