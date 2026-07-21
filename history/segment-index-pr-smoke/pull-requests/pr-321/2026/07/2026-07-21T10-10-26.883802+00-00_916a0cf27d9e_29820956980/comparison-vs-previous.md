# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `827657fe930eb1d550431967aadf932ddf12455c`
- Candidate SHA: `916a0cf27d9e1326d5231f350e50b16f2c10d6ac`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2354513.930 ops/s` | `2342513.896 ops/s` | `-0.51%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2395251.363 ops/s` | `2033795.871 ops/s` | `-15.09%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2095340.741 ops/s` | `2106611.977 ops/s` | `+0.54%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2029333.075 ops/s` | `2211591.450 ops/s` | `+8.98%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2318651.441 ops/s` | `2323458.730 ops/s` | `+0.21%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1145276.745 ops/s` | `1139301.834 ops/s` | `-0.52%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `460492.559 ops/s` | `472118.687 ops/s` | `+2.52%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `298175.295 ops/s` | `307254.336 ops/s` | `+3.04%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162317.264 ops/s` | `164864.351 ops/s` | `+1.57%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `484164.335 ops/s` | `610933.765 ops/s` | `+26.18%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `471440.968 ops/s` | `595735.204 ops/s` | `+26.36%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12723.368 ops/s` | `15198.561 ops/s` | `+19.45%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `-` | `2675.810 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:putSync` | `-` | `2676.204 ops/s` | `-` | `new` |
