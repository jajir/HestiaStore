# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `beed42bba1c8ad9ec96ed6706ff7c251ea7f9963`
- Candidate SHA: `5686935ec54ff8b29fa038ca0639ff92bf24a671`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4198972.349 ops/s` | `2621402.511 ops/s` | `-37.57%` | `worse` |
| `segment-index-get-live:getMissSync` | `4793663.087 ops/s` | `4637468.959 ops/s` | `-3.26%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `86.804 ops/s` | `80.973 ops/s` | `-6.72%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4561537.063 ops/s` | `4686879.051 ops/s` | `+2.75%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `173867.881 ops/s` | `156708.479 ops/s` | `-9.87%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4664838.612 ops/s` | `4636993.716 ops/s` | `-0.60%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `3245091.245 ops/s` | `2799683.551 ops/s` | `-13.73%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1960839.394 ops/s` | `1529929.203 ops/s` | `-21.98%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `382381.162 ops/s` | `357687.266 ops/s` | `-6.46%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `144820.985 ops/s` | `113027.720 ops/s` | `-21.95%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `237560.177 ops/s` | `244659.546 ops/s` | `+2.99%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `7558.379 ops/s` | `253460.170 ops/s` | `+3253.37%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `2253.402 ops/s` | `8185.344 ops/s` | `+263.24%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5304.977 ops/s` | `245274.826 ops/s` | `+4523.48%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1666.153 ops/s` | `2255.113 ops/s` | `+35.35%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1637.640 ops/s` | `2194.954 ops/s` | `+34.03%` | `better` |
