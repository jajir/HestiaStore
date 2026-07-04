# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2159606.884 ops/s` | `2148261.903 ops/s` | `-0.53%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1982843.053 ops/s` | `2066817.157 ops/s` | `+4.24%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1534451.389 ops/s` | `1647331.880 ops/s` | `+7.36%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1997851.311 ops/s` | `1955273.283 ops/s` | `-2.13%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1870211.846 ops/s` | `2149378.123 ops/s` | `+14.93%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1107115.216 ops/s` | `1054790.189 ops/s` | `-4.73%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `260.307 ms/op` | `259.211 ms/op` | `-0.42%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `280.515 ms/op` | `283.105 ms/op` | `+0.92%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `254.209 ms/op` | `257.447 ms/op` | `+1.27%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `436709.943 ops/s` | `447130.050 ops/s` | `+2.39%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `200494.492 ops/s` | `204764.785 ops/s` | `+2.13%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `236215.451 ops/s` | `242365.265 ops/s` | `+2.60%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `891596.165 ops/s` | `844196.525 ops/s` | `-5.32%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `875258.289 ops/s` | `827387.017 ops/s` | `-5.47%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16337.877 ops/s` | `16809.508 ops/s` | `+2.89%` | `neutral` |
