# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9c725bd833aba4b408c00a1771c1bb051d786844`
- Candidate SHA: `e13f1423948b75fff547281df46c96dd9d1f5528`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2380577.040 ops/s` | `2054743.165 ops/s` | `-13.69%` | `worse` |
| `segment-index-get-live:getMissSync` | `4168570.677 ops/s` | `3769882.069 ops/s` | `-9.56%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.259 ops/s` | `74.710 ops/s` | `-11.33%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3987123.238 ops/s` | `3534346.418 ops/s` | `-11.36%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `120076.830 ops/s` | `109421.666 ops/s` | `-8.87%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4036205.462 ops/s` | `4019055.890 ops/s` | `-0.42%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2178938.511 ops/s` | `2234227.026 ops/s` | `+2.54%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1161093.659 ops/s` | `1095008.452 ops/s` | `-5.69%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `330753.215 ops/s` | `307643.581 ops/s` | `-6.99%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `165890.561 ops/s` | `143867.172 ops/s` | `-13.28%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `164862.654 ops/s` | `163776.409 ops/s` | `-0.66%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `64134.222 ops/s` | `12504.714 ops/s` | `-80.50%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `5375.826 ops/s` | `9158.273 ops/s` | `+70.36%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `58758.396 ops/s` | `3346.441 ops/s` | `-94.30%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `1964.591 ops/s` | `2300.106 ops/s` | `+17.08%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2175.646 ops/s` | `2116.385 ops/s` | `-2.72%` | `neutral` |
