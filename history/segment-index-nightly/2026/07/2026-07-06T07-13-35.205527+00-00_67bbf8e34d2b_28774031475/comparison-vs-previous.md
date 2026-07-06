# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2238231.709 ops/s` | `2142081.833 ops/s` | `-4.30%` | `warning` |
| `segment-index-get-live:getMissSync` | `2099355.991 ops/s` | `1860645.684 ops/s` | `-11.37%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `1499259.094 ops/s` | `1574327.310 ops/s` | `+5.01%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1879184.453 ops/s` | `2034475.313 ops/s` | `+8.26%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2028034.926 ops/s` | `2113823.865 ops/s` | `+4.23%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1105632.844 ops/s` | `1079618.243 ops/s` | `-2.35%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `263.198 ms/op` | `263.418 ms/op` | `+0.08%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `287.121 ms/op` | `281.955 ms/op` | `-1.80%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `256.524 ms/op` | `258.928 ms/op` | `+0.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `413179.309 ops/s` | `440263.041 ops/s` | `+6.55%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `198652.297 ops/s` | `205029.507 ops/s` | `+3.21%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `214527.012 ops/s` | `235233.534 ops/s` | `+9.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `869601.310 ops/s` | `923939.768 ops/s` | `+6.25%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `852662.058 ops/s` | `907996.370 ops/s` | `+6.49%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16939.251 ops/s` | `15943.398 ops/s` | `-5.88%` | `warning` |
