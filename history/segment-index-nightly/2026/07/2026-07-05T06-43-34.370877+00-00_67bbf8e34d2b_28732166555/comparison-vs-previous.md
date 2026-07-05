# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1960617.011 ops/s` | `2309543.104 ops/s` | `+17.80%` | `better` |
| `segment-index-get-live:getMissSync` | `1622916.974 ops/s` | `1826959.283 ops/s` | `+12.57%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1242233.206 ops/s` | `1469885.013 ops/s` | `+18.33%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1675844.265 ops/s` | `1630756.875 ops/s` | `-2.69%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1907004.668 ops/s` | `1930626.717 ops/s` | `+1.24%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `885177.410 ops/s` | `992183.035 ops/s` | `+12.09%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `282.280 ms/op` | `280.622 ms/op` | `-0.59%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `306.054 ms/op` | `305.209 ms/op` | `-0.28%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `279.537 ms/op` | `275.445 ms/op` | `-1.46%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `383908.609 ops/s` | `381001.261 ops/s` | `-0.76%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `178679.765 ops/s` | `175648.229 ops/s` | `-1.70%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `205228.844 ops/s` | `205353.031 ops/s` | `+0.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `742397.476 ops/s` | `767694.421 ops/s` | `+3.41%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `727277.534 ops/s` | `752007.631 ops/s` | `+3.40%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15119.942 ops/s` | `15686.791 ops/s` | `+3.75%` | `better` |
