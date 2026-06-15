# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1912aeb3cdf5d5e5748b841b244c8640aad43d62`
- Candidate SHA: `af378b9d5c3a2f71cd46240553eb253bc998f6e7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2181586.932 ops/s` | `2122766.669 ops/s` | `-2.70%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2159802.718 ops/s` | `2102919.188 ops/s` | `-2.63%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1913999.946 ops/s` | `1952170.882 ops/s` | `+1.99%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1986312.771 ops/s` | `2189764.579 ops/s` | `+10.24%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2046848.729 ops/s` | `2020647.843 ops/s` | `-1.28%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1129551.128 ops/s` | `1112846.968 ops/s` | `-1.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `312409.026 ops/s` | `314192.876 ops/s` | `+0.57%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `164910.741 ops/s` | `143614.830 ops/s` | `-12.91%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `147498.285 ops/s` | `170578.046 ops/s` | `+15.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `182455.975 ops/s` | `178998.678 ops/s` | `-1.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `167939.649 ops/s` | `164548.614 ops/s` | `-2.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14516.326 ops/s` | `14450.063 ops/s` | `-0.46%` | `neutral` |
