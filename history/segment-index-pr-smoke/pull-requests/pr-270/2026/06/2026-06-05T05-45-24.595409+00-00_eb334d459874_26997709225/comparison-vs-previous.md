# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `050681c55cf12c88b63d434fbb3236e80666bd7b`
- Candidate SHA: `eb334d4598748b47e876a30f2e40667097884359`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1783242.907 ops/s` | `1814068.011 ops/s` | `+1.73%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1782373.898 ops/s` | `1746714.826 ops/s` | `-2.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1748212.146 ops/s` | `1708466.990 ops/s` | `-2.27%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1799983.961 ops/s` | `1822722.536 ops/s` | `+1.26%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1873444.039 ops/s` | `1850841.992 ops/s` | `-1.21%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1047091.134 ops/s` | `1137371.384 ops/s` | `+8.62%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `252418.076 ops/s` | `254445.396 ops/s` | `+0.80%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `92489.001 ops/s` | `102184.473 ops/s` | `+10.48%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `159929.075 ops/s` | `152260.923 ops/s` | `-4.79%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `161582.855 ops/s` | `168132.273 ops/s` | `+4.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `146500.852 ops/s` | `154354.041 ops/s` | `+5.36%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15082.003 ops/s` | `13778.232 ops/s` | `-8.64%` | `worse` |
