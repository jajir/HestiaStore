# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d071c9602c6022fca98dfbd1e869ca129e4cd557`
- Candidate SHA: `8b67d20c8e7a9c3b624c21f46367f66c2cba5b7a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2146251.189 ops/s` | `2752230.799 ops/s` | `+28.23%` | `better` |
| `segment-index-get-live:getMissSync` | `3623888.505 ops/s` | `4412042.592 ops/s` | `+21.75%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7590.125 ops/s` | `8208.915 ops/s` | `+8.15%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3557139.759 ops/s` | `4348092.781 ops/s` | `+22.24%` | `better` |
| `segment-index-get-persisted:getHitSync` | `121505.264 ops/s` | `162137.630 ops/s` | `+33.44%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3532808.625 ops/s` | `4435914.974 ops/s` | `+25.56%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1965836.839 ops/s` | `2794290.237 ops/s` | `+42.14%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1089192.007 ops/s` | `1361856.349 ops/s` | `+25.03%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285913.041 ops/s` | `361941.234 ops/s` | `+26.59%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `107818.573 ops/s` | `158923.750 ops/s` | `+47.40%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `178094.468 ops/s` | `203017.484 ops/s` | `+13.99%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `49402.235 ops/s` | `63443.764 ops/s` | `+28.42%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `44096.518 ops/s` | `57975.249 ops/s` | `+31.47%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5305.717 ops/s` | `5468.516 ops/s` | `+3.07%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3460.051 ops/s` | `1667.553 ops/s` | `-51.81%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `457.164 ops/s` | `435.044 ops/s` | `-4.84%` | `warning` |
