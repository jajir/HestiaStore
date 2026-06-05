# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9633e2a4913af4151377977c1ea77f428d59861d`
- Candidate SHA: `050681c55cf12c88b63d434fbb3236e80666bd7b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2175841.099 ops/s` | `2198999.419 ops/s` | `+1.06%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2008583.424 ops/s` | `1883979.708 ops/s` | `-6.20%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1874643.168 ops/s` | `2027696.810 ops/s` | `+8.16%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1974621.309 ops/s` | `2064501.624 ops/s` | `+4.55%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2076539.043 ops/s` | `2169693.771 ops/s` | `+4.49%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1058478.404 ops/s` | `1096910.489 ops/s` | `+3.63%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `288909.320 ops/s` | `322734.231 ops/s` | `+11.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `129802.203 ops/s` | `147635.328 ops/s` | `+13.74%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `159107.117 ops/s` | `175098.902 ops/s` | `+10.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `187823.037 ops/s` | `167080.068 ops/s` | `-11.04%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `174150.248 ops/s` | `151892.169 ops/s` | `-12.78%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13672.789 ops/s` | `15187.899 ops/s` | `+11.08%` | `better` |
