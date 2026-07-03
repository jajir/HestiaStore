# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2110640.897 ops/s` | `2203125.122 ops/s` | `+4.38%` | `better` |
| `segment-index-get-live:getMissSync` | `1815524.483 ops/s` | `1976603.738 ops/s` | `+8.87%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1622946.643 ops/s` | `1818834.159 ops/s` | `+12.07%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1873126.530 ops/s` | `1866197.116 ops/s` | `-0.37%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1954321.046 ops/s` | `2070123.033 ops/s` | `+5.93%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1122330.517 ops/s` | `1108579.887 ops/s` | `-1.23%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `254.938 ms/op` | `252.929 ms/op` | `-0.79%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `277.798 ms/op` | `277.428 ms/op` | `-0.13%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `250.798 ms/op` | `252.144 ms/op` | `+0.54%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `435795.053 ops/s` | `447347.758 ops/s` | `+2.65%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `191333.060 ops/s` | `206956.597 ops/s` | `+8.17%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `244461.993 ops/s` | `240391.161 ops/s` | `-1.67%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `861828.728 ops/s` | `908617.089 ops/s` | `+5.43%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `845107.241 ops/s` | `891573.074 ops/s` | `+5.50%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16721.486 ops/s` | `17044.015 ops/s` | `+1.93%` | `neutral` |
