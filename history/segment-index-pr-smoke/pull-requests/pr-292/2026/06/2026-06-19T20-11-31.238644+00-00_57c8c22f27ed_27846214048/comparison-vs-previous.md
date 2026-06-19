# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7cf45898d1c4e656791b1a025e36fee94e8195ca`
- Candidate SHA: `57c8c22f27edc91e058ab578785449ef7d09abd8`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2128126.466 ops/s` | `2192627.612 ops/s` | `+3.03%` | `better` |
| `segment-index-get-live:getMissSync` | `1991060.379 ops/s` | `2081715.138 ops/s` | `+4.55%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1856210.310 ops/s` | `1851532.215 ops/s` | `-0.25%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2079199.722 ops/s` | `2073386.374 ops/s` | `-0.28%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1996746.255 ops/s` | `2141556.991 ops/s` | `+7.25%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1087152.983 ops/s` | `1077662.654 ops/s` | `-0.87%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `292321.295 ops/s` | `293577.359 ops/s` | `+0.43%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `121447.723 ops/s` | `123314.967 ops/s` | `+1.54%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170873.572 ops/s` | `170262.391 ops/s` | `-0.36%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `175384.536 ops/s` | `179833.079 ops/s` | `+2.54%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `161442.500 ops/s` | `164894.951 ops/s` | `+2.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13942.035 ops/s` | `14938.128 ops/s` | `+7.14%` | `better` |
