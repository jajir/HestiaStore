# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `73b0861c316726a3965ae86a737f820524e94f55`
- Candidate SHA: `827657fe930eb1d550431967aadf932ddf12455c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1742835.182 ops/s` | `1924043.550 ops/s` | `+10.40%` | `better` |
| `segment-index-get-live:getMissSync` | `1600116.798 ops/s` | `1655724.876 ops/s` | `+3.48%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1589269.122 ops/s` | `1624602.637 ops/s` | `+2.22%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1608437.304 ops/s` | `1633121.896 ops/s` | `+1.53%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2044973.113 ops/s` | `2252212.100 ops/s` | `+10.13%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1011786.528 ops/s` | `1082966.493 ops/s` | `+7.04%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `396295.650 ops/s` | `435746.692 ops/s` | `+9.95%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `219974.425 ops/s` | `255788.034 ops/s` | `+16.28%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `176321.225 ops/s` | `179958.657 ops/s` | `+2.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `566026.835 ops/s` | `716835.624 ops/s` | `+26.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `550040.674 ops/s` | `699307.990 ops/s` | `+27.14%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15986.161 ops/s` | `17527.634 ops/s` | `+9.64%` | `better` |
