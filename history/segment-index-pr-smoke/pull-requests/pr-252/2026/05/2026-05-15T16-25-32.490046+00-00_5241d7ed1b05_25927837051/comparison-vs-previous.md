# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1779e5cbda1582ee2e64faf5e42ccf7ebb7beb13`
- Candidate SHA: `5241d7ed1b05118174a0124df70d4c935ac364dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2113840.152 ops/s` | `2321379.218 ops/s` | `+9.82%` | `better` |
| `segment-index-get-live:getMissSync` | `3447394.821 ops/s` | `3644450.610 ops/s` | `+5.72%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7336.164 ops/s` | `7338.156 ops/s` | `+0.03%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3560233.841 ops/s` | `3487650.459 ops/s` | `-2.04%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1915852.619 ops/s` | `1740822.902 ops/s` | `-9.14%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3555633.022 ops/s` | `3396520.300 ops/s` | `-4.47%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1807974.602 ops/s` | `1943052.496 ops/s` | `+7.47%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1015759.932 ops/s` | `1075834.999 ops/s` | `+5.91%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `269954.144 ops/s` | `286645.801 ops/s` | `+6.18%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `118388.012 ops/s` | `115702.837 ops/s` | `-2.27%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `151566.132 ops/s` | `170942.964 ops/s` | `+12.78%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `42314.743 ops/s` | `40910.583 ops/s` | `-3.32%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `37066.952 ops/s` | `35745.840 ops/s` | `-3.56%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5247.791 ops/s` | `5164.743 ops/s` | `-1.58%` | `neutral` |
