# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `2269079a2d2cb8a4ff9d6120a27b647ace492203`
- Candidate SHA: `96250d2ebd14a614ff9d5addef1d3fe77fd7a921`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2235922.947 ops/s` | `2377538.169 ops/s` | `+6.33%` | `better` |
| `segment-index-get-live:getMissSync` | `3891721.509 ops/s` | `2109717.874 ops/s` | `-45.79%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `13007.101 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-multisegment-hot:getMissSync` | `3768752.239 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-persisted:getHitSync` | `2048383.851 ops/s` | `2265348.317 ops/s` | `+10.59%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3794412.191 ops/s` | `2042782.044 ops/s` | `-46.16%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2110206.270 ops/s` | `1754199.196 ops/s` | `-16.87%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1021066.462 ops/s` | `1107597.791 ops/s` | `+8.47%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `295033.981 ops/s` | `287565.824 ops/s` | `-2.53%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `133638.329 ops/s` | `116497.010 ops/s` | `-12.83%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161395.652 ops/s` | `171068.814 ops/s` | `+5.99%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `53265.164 ops/s` | `139979.700 ops/s` | `+162.80%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `47806.426 ops/s` | `123468.917 ops/s` | `+158.27%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5458.737 ops/s` | `16510.783 ops/s` | `+202.47%` | `better` |
