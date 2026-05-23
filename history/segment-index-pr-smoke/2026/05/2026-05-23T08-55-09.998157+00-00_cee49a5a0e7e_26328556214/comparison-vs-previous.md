# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `b231d8fac75bf2d77ae759c5fa08f6253e89e553`
- Candidate SHA: `cee49a5a0e7ec40a3b8ed557dd1b8b2dc5e22511`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2159225.574 ops/s` | `2197456.451 ops/s` | `+1.77%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1849266.041 ops/s` | `2145497.122 ops/s` | `+16.02%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `1993912.463 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitSync` | `1541664.536 ops/s` | `1844420.245 ops/s` | `+19.64%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1925575.270 ops/s` | `2098524.694 ops/s` | `+8.98%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2007444.905 ops/s` | `1929642.793 ops/s` | `-3.88%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1028064.495 ops/s` | `1101279.042 ops/s` | `+7.12%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `267082.229 ops/s` | `276575.314 ops/s` | `+3.55%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `100135.907 ops/s` | `124257.016 ops/s` | `+24.09%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `166946.322 ops/s` | `152318.298 ops/s` | `-8.76%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `164721.556 ops/s` | `180747.317 ops/s` | `+9.73%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `149732.078 ops/s` | `166618.079 ops/s` | `+11.28%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14989.478 ops/s` | `14129.237 ops/s` | `-5.74%` | `warning` |
