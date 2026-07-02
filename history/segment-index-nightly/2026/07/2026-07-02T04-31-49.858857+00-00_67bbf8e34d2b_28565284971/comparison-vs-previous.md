# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2094835.154 ops/s` | `2179188.891 ops/s` | `+4.03%` | `better` |
| `segment-index-get-live:getMissSync` | `1886256.826 ops/s` | `1917384.768 ops/s` | `+1.65%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1855592.474 ops/s` | `1922268.279 ops/s` | `+3.59%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2021153.631 ops/s` | `2145836.406 ops/s` | `+6.17%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1848780.652 ops/s` | `1948603.121 ops/s` | `+5.40%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1113583.387 ops/s` | `1105225.110 ops/s` | `-0.75%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `239.583 ms/op` | `244.545 ms/op` | `+2.07%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `263.875 ms/op` | `267.007 ms/op` | `+1.19%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.726 ms/op` | `238.002 ms/op` | `-1.13%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448000.450 ops/s` | `434711.216 ops/s` | `-2.97%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `213664.465 ops/s` | `194990.432 ops/s` | `-8.74%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `234335.985 ops/s` | `239720.784 ops/s` | `+2.30%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `883702.551 ops/s` | `866958.410 ops/s` | `-1.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `867005.952 ops/s` | `849697.207 ops/s` | `-2.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16696.598 ops/s` | `17261.203 ops/s` | `+3.38%` | `better` |
