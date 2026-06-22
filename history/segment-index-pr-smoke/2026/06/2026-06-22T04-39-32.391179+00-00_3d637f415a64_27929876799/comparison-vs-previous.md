# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `cb23b15e3542f2dcf618e20cb692494f55e16165`
- Candidate SHA: `3d637f415a64eb5d9442f83f8a02f91fa1e556ba`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2469210.137 ops/s` | `2175357.086 ops/s` | `-11.90%` | `worse` |
| `segment-index-get-live:getMissSync` | `2054169.452 ops/s` | `1977296.808 ops/s` | `-3.74%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1750801.089 ops/s` | `1925619.455 ops/s` | `+9.99%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2086475.699 ops/s` | `2093842.465 ops/s` | `+0.35%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1998749.249 ops/s` | `2112233.198 ops/s` | `+5.68%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1091455.285 ops/s` | `1134256.274 ops/s` | `+3.92%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `302610.570 ops/s` | `282464.296 ops/s` | `-6.66%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `136674.488 ops/s` | `126536.491 ops/s` | `-7.42%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165936.082 ops/s` | `155927.805 ops/s` | `-6.03%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `190701.583 ops/s` | `192114.116 ops/s` | `+0.74%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `176478.243 ops/s` | `177482.576 ops/s` | `+0.57%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14223.340 ops/s` | `14631.539 ops/s` | `+2.87%` | `neutral` |
