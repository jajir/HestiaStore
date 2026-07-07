# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2135000.220 ops/s` | `2160421.052 ops/s` | `+1.19%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1796901.307 ops/s` | `1915722.792 ops/s` | `+6.61%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1637252.619 ops/s` | `1705917.172 ops/s` | `+4.19%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1915133.168 ops/s` | `1855359.091 ops/s` | `-3.12%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2110273.809 ops/s` | `2178202.835 ops/s` | `+3.22%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1119230.908 ops/s` | `1111368.375 ops/s` | `-0.70%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `261.714 ms/op` | `258.082 ms/op` | `-1.39%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `274.378 ms/op` | `281.071 ms/op` | `+2.44%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `255.435 ms/op` | `256.232 ms/op` | `+0.31%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `427547.183 ops/s` | `436972.460 ops/s` | `+2.20%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `190960.487 ops/s` | `198177.492 ops/s` | `+3.78%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `236586.696 ops/s` | `238794.968 ops/s` | `+0.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `883751.677 ops/s` | `857223.814 ops/s` | `-3.00%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `866594.922 ops/s` | `840515.706 ops/s` | `-3.01%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17156.755 ops/s` | `16708.108 ops/s` | `-2.61%` | `neutral` |
