# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ca5ac256510e397ee600cb412d96041ce85384b9`
- Candidate SHA: `418a34d14b9e250d657339c54e1160b78f6a4bd3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2661595.426 ops/s` | `2804647.338 ops/s` | `+5.37%` | `better` |
| `segment-index-get-live:getMissSync` | `4472253.922 ops/s` | `4561127.882 ops/s` | `+1.99%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `8259.061 ops/s` | `7903.956 ops/s` | `-4.30%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4754921.698 ops/s` | `4303004.420 ops/s` | `-9.50%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `173185.512 ops/s` | `156068.371 ops/s` | `-9.88%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4830946.139 ops/s` | `4514240.642 ops/s` | `-6.56%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2896342.990 ops/s` | `2759677.964 ops/s` | `-4.72%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1435754.750 ops/s` | `1373863.517 ops/s` | `-4.31%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `326708.523 ops/s` | `350625.943 ops/s` | `+7.32%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `147435.164 ops/s` | `115908.118 ops/s` | `-21.38%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `179273.359 ops/s` | `234717.825 ops/s` | `+30.93%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `87575.671 ops/s` | `66331.653 ops/s` | `-24.26%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `71806.299 ops/s` | `60815.702 ops/s` | `-15.31%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15769.372 ops/s` | `5515.951 ops/s` | `-65.02%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `591.043 ops/s` | `535.685 ops/s` | `-9.37%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `1322.967 ops/s` | `116.374 ops/s` | `-91.20%` | `worse` |
