# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `cee49a5a0e7ec40a3b8ed557dd1b8b2dc5e22511`
- Candidate SHA: `fe8ed43da7b5fdfc20779080081fb61f81e202b6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2232921.695 ops/s` | `2225227.880 ops/s` | `-0.34%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2140657.161 ops/s` | `2086128.074 ops/s` | `-2.55%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `12579.670 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-persisted:getHitSync` | `1976909.927 ops/s` | `1819389.390 ops/s` | `-7.97%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2179135.583 ops/s` | `2211019.408 ops/s` | `+1.46%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1969295.815 ops/s` | `2125074.590 ops/s` | `+7.91%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1103250.999 ops/s` | `1157476.827 ops/s` | `+4.92%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285865.245 ops/s` | `318275.313 ops/s` | `+11.34%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `147281.785 ops/s` | `150721.562 ops/s` | `+2.34%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `138583.460 ops/s` | `167553.751 ops/s` | `+20.90%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `151256.406 ops/s` | `175463.625 ops/s` | `+16.00%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `137508.020 ops/s` | `161991.650 ops/s` | `+17.81%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13748.386 ops/s` | `13471.975 ops/s` | `-2.01%` | `neutral` |
