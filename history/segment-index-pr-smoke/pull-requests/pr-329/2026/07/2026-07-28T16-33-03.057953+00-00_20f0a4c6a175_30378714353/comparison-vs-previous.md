# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6802a1ecd057ef44daba17179d663834b289f16a`
- Candidate SHA: `20f0a4c6a17542e8d5852728d3a39b657f17a79c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4799263.386 ops/s` | `7000455.994 ops/s` | `+45.87%` | `better` |
| `segment-index-get-live:getMissSync` | `4381866.561 ops/s` | `7310752.937 ops/s` | `+66.84%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3280479.027 ops/s` | `5752702.359 ops/s` | `+75.36%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4434571.463 ops/s` | `6861903.040 ops/s` | `+54.74%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3437513.061 ops/s` | `5626871.209 ops/s` | `+63.69%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4317225.295 ops/s` | `6916150.568 ops/s` | `+60.20%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3980864.571 ops/s` | `6128640.922 ops/s` | `+53.95%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2082054.860 ops/s` | `3192931.817 ops/s` | `+53.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `583547.567 ops/s` | `719269.843 ops/s` | `+23.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `414579.786 ops/s` | `436539.035 ops/s` | `+5.30%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168967.781 ops/s` | `282730.809 ops/s` | `+67.33%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `811158.630 ops/s` | `1832196.692 ops/s` | `+125.87%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `797465.345 ops/s` | `1811814.357 ops/s` | `+127.20%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13693.285 ops/s` | `20382.335 ops/s` | `+48.85%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7241.097 ops/s` | `1612.408 ops/s` | `-77.73%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7342.690 ops/s` | `2517.413 ops/s` | `-65.72%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3345.246 ops/s` | `422.663 ops/s` | `-87.37%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3399.733 ops/s` | `630.432 ops/s` | `-81.46%` | `worse` |
