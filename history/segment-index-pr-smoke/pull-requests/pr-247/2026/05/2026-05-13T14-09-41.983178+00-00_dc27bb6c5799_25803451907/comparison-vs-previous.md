# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7b4e3e01818862a8152f92756c8f59b19c59d7b3`
- Candidate SHA: `dc27bb6c5799de544f9a3c16faa39dfeb4086d24`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2205680.654 ops/s` | `2279566.748 ops/s` | `+3.35%` | `better` |
| `segment-index-get-live:getMissSync` | `3444415.845 ops/s` | `3680898.173 ops/s` | `+6.87%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `8184.441 ops/s` | `6992.389 ops/s` | `-14.56%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3607849.531 ops/s` | `3516747.188 ops/s` | `-2.53%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103579.525 ops/s` | `2109457.487 ops/s` | `+1936.56%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3649495.118 ops/s` | `3472748.858 ops/s` | `-4.84%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1937266.184 ops/s` | `2091883.572 ops/s` | `+7.98%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1066372.834 ops/s` | `1062854.188 ops/s` | `-0.33%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `321046.318 ops/s` | `295995.059 ops/s` | `-7.80%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `158929.827 ops/s` | `127462.093 ops/s` | `-19.80%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162116.491 ops/s` | `168532.965 ops/s` | `+3.96%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `47264.218 ops/s` | `41048.739 ops/s` | `-13.15%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `41988.860 ops/s` | `35802.907 ops/s` | `-14.73%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5275.358 ops/s` | `5245.831 ops/s` | `-0.56%` | `neutral` |
