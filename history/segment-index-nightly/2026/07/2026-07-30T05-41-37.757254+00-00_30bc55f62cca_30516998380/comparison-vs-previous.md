# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `30bc55f62cca333dbbb24dd3a1d598ed4e18153b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1799066.716 ops/s` | `2027136.907 ops/s` | `+12.68%` | `better` |
| `segment-index-get-live:getMissSync` | `1811257.205 ops/s` | `2195299.414 ops/s` | `+21.20%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1452440.989 ops/s` | `1998308.074 ops/s` | `+37.58%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1910822.154 ops/s` | `2019592.448 ops/s` | `+5.69%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2121602.514 ops/s` | `2138162.659 ops/s` | `+0.78%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1053268.199 ops/s` | `1032699.794 ops/s` | `-1.95%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `310.408 ms/op` | `308.455 ms/op` | `-0.63%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `331.419 ms/op` | `330.446 ms/op` | `-0.29%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `306.477 ms/op` | `309.933 ms/op` | `+1.13%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `432279.221 ops/s` | `452421.442 ops/s` | `+4.66%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `186025.606 ops/s` | `206366.130 ops/s` | `+10.93%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `246253.615 ops/s` | `246055.311 ops/s` | `-0.08%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `862300.331 ops/s` | `817998.710 ops/s` | `-5.14%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `843740.876 ops/s` | `800588.594 ops/s` | `-5.11%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18559.455 ops/s` | `17410.117 ops/s` | `-6.19%` | `warning` |
