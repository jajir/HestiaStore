# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `3d637f415a64eb5d9442f83f8a02f91fa1e556ba`
- Candidate SHA: `e4db0089603e48f1404346f946cc242f94e6d4ed`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2271211.151 ops/s` | `2141826.963 ops/s` | `-5.70%` | `warning` |
| `segment-index-get-live:getMissSync` | `1952698.078 ops/s` | `1897589.518 ops/s` | `-2.82%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2021224.360 ops/s` | `1668219.502 ops/s` | `-17.46%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2136804.563 ops/s` | `2098447.099 ops/s` | `-1.80%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2022349.691 ops/s` | `2104681.306 ops/s` | `+4.07%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1105759.233 ops/s` | `1101383.017 ops/s` | `-0.40%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `297754.473 ops/s` | `292927.261 ops/s` | `-1.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `134041.446 ops/s` | `136707.184 ops/s` | `+1.99%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `163713.026 ops/s` | `156220.077 ops/s` | `-4.58%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `176785.265 ops/s` | `183106.095 ops/s` | `+3.58%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `162733.240 ops/s` | `167974.706 ops/s` | `+3.22%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14052.024 ops/s` | `15131.388 ops/s` | `+7.68%` | `better` |
