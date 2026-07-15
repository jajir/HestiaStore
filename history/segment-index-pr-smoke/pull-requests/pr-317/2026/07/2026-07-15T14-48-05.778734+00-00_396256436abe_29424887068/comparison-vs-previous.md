# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Candidate SHA: `396256436abefd12881c219f7068f850abea1526`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2159817.644 ops/s` | `2027819.772 ops/s` | `-6.11%` | `warning` |
| `segment-index-get-live:getMissSync` | `2085400.152 ops/s` | `2164161.578 ops/s` | `+3.78%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1800506.235 ops/s` | `1838033.375 ops/s` | `+2.08%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2001664.575 ops/s` | `2072537.835 ops/s` | `+3.54%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2042816.085 ops/s` | `2164252.804 ops/s` | `+5.94%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1107865.236 ops/s` | `1101764.748 ops/s` | `-0.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `462359.025 ops/s` | `441287.740 ops/s` | `-4.56%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `288180.634 ops/s` | `271621.945 ops/s` | `-5.75%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `174178.391 ops/s` | `169665.795 ops/s` | `-2.59%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `681248.389 ops/s` | `545187.604 ops/s` | `-19.97%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `665555.373 ops/s` | `531767.846 ops/s` | `-20.10%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15693.016 ops/s` | `13419.758 ops/s` | `-14.49%` | `worse` |
