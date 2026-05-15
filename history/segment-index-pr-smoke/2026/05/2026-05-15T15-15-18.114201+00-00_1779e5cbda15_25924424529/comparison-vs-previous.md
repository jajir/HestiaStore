# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `042a54fc5b39a821f487e959141a4dd5b63ff8e7`
- Candidate SHA: `1779e5cbda1582ee2e64faf5e42ccf7ebb7beb13`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2256378.436 ops/s` | `2244122.094 ops/s` | `-0.54%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3953086.485 ops/s` | `3665402.720 ops/s` | `-7.28%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7002.570 ops/s` | `6867.221 ops/s` | `-1.93%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3535568.090 ops/s` | `3790127.085 ops/s` | `+7.20%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1847732.747 ops/s` | `1885652.946 ops/s` | `+2.05%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3800960.439 ops/s` | `3711494.656 ops/s` | `-2.35%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1901404.532 ops/s` | `1852047.879 ops/s` | `-2.60%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `982768.922 ops/s` | `983527.693 ops/s` | `+0.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `284077.686 ops/s` | `287594.552 ops/s` | `+1.24%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `127782.528 ops/s` | `133387.212 ops/s` | `+4.39%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `156295.158 ops/s` | `154207.340 ops/s` | `-1.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `34911.981 ops/s` | `35229.396 ops/s` | `+0.91%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `29725.223 ops/s` | `29989.709 ops/s` | `+0.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5186.758 ops/s` | `5239.687 ops/s` | `+1.02%` | `neutral` |
