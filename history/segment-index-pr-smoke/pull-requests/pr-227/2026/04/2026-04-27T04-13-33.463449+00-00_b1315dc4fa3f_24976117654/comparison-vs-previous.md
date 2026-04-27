# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ca5ac256510e397ee600cb412d96041ce85384b9`
- Candidate SHA: `b1315dc4fa3f83aa0b23ad299c94f0148b1c954d`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2018427.574 ops/s` | `2409504.325 ops/s` | `+19.38%` | `better` |
| `segment-index-get-live:getMissSync` | `3829936.534 ops/s` | `3852186.242 ops/s` | `+0.58%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7068.012 ops/s` | `6697.095 ops/s` | `-5.25%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3934069.692 ops/s` | `3888409.641 ops/s` | `-1.16%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116612.016 ops/s` | `106483.431 ops/s` | `-8.69%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3870065.221 ops/s` | `3865046.947 ops/s` | `-0.13%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2125806.714 ops/s` | `2077983.425 ops/s` | `-2.25%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1126465.010 ops/s` | `1115862.469 ops/s` | `-0.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `312656.587 ops/s` | `287695.224 ops/s` | `-7.98%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `182965.066 ops/s` | `122993.511 ops/s` | `-32.78%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `129691.521 ops/s` | `164701.713 ops/s` | `+26.99%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `38932.940 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `33666.516 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `5266.425 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:deleteSync` | `2824.266 ops/s` | `2751.879 ops/s` | `-2.56%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2762.508 ops/s` | `456.808 ops/s` | `-83.46%` | `worse` |
