# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e181bacb91161177cbeddbbc4d92d9884c1095d5`
- Candidate SHA: `8a2e0e86108aa5584249615279c662781d44cf08`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2247739.930 ops/s` | `4530636.177 ops/s` | `+101.56%` | `better` |
| `segment-index-get-live:getMissSync` | `2182687.634 ops/s` | `4462698.841 ops/s` | `+104.46%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2150653.098 ops/s` | `3376287.333 ops/s` | `+56.99%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2272121.894 ops/s` | `4499263.846 ops/s` | `+98.02%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2259264.678 ops/s` | `3443422.604 ops/s` | `+52.41%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1125394.260 ops/s` | `1910157.495 ops/s` | `+69.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `479608.986 ops/s` | `567371.740 ops/s` | `+18.30%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `311128.134 ops/s` | `394958.371 ops/s` | `+26.94%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168480.853 ops/s` | `172413.369 ops/s` | `+2.33%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `735846.064 ops/s` | `740656.784 ops/s` | `+0.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `718806.480 ops/s` | `726750.661 ops/s` | `+1.11%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17039.584 ops/s` | `13906.124 ops/s` | `-18.39%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3652.264 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `3497.884 ops/s` | `-` | `-` | `removed` |
