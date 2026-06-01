# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `afc62792cea444b4d3f438b5c4f0ffbce30e8371`
- Candidate SHA: `9b32d2f1f70d75214a75369472d50cd4b4736a6f`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2369658.643 ops/s` | `2306052.709 ops/s` | `-2.68%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1872840.935 ops/s` | `2251532.218 ops/s` | `+20.22%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1937768.451 ops/s` | `1551284.971 ops/s` | `-19.94%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2172366.696 ops/s` | `2153917.255 ops/s` | `-0.85%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2072859.937 ops/s` | `2172876.408 ops/s` | `+4.83%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1118594.012 ops/s` | `1136329.601 ops/s` | `+1.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `294606.833 ops/s` | `302720.665 ops/s` | `+2.75%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `146026.916 ops/s` | `165177.895 ops/s` | `+13.11%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `148579.918 ops/s` | `137542.770 ops/s` | `-7.43%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `157001.849 ops/s` | `164760.854 ops/s` | `+4.94%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `143537.738 ops/s` | `149787.279 ops/s` | `+4.35%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13464.111 ops/s` | `14973.574 ops/s` | `+11.21%` | `better` |
