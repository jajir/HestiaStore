# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c0ab96dedfed051af6401d8322b0e7c046e10a49`
- Candidate SHA: `9b036e9f35c6002c6b57f67843be2ae99ff30bc7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2530263.942 ops/s` | `5168396.757 ops/s` | `+104.26%` | `better` |
| `segment-index-get-live:getMissSync` | `2739626.310 ops/s` | `4648337.153 ops/s` | `+69.67%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1969403.324 ops/s` | `3588512.373 ops/s` | `+82.21%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2842652.580 ops/s` | `4742795.904 ops/s` | `+66.84%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2073945.683 ops/s` | `3225010.258 ops/s` | `+55.50%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2964942.676 ops/s` | `4588423.711 ops/s` | `+54.76%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2479638.879 ops/s` | `4320821.089 ops/s` | `+74.25%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1343640.151 ops/s` | `2215394.384 ops/s` | `+64.88%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `469205.336 ops/s` | `558383.020 ops/s` | `+19.01%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `281163.577 ops/s` | `386272.359 ops/s` | `+37.38%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `188041.758 ops/s` | `172110.661 ops/s` | `-8.47%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `794823.360 ops/s` | `778156.996 ops/s` | `-2.10%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `779877.493 ops/s` | `764422.397 ops/s` | `-1.98%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14945.867 ops/s` | `13734.599 ops/s` | `-8.10%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `4257.990 ops/s` | `6302.120 ops/s` | `+48.01%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3839.772 ops/s` | `6428.351 ops/s` | `+67.41%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1973.337 ops/s` | `2526.324 ops/s` | `+28.02%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1827.851 ops/s` | `2425.430 ops/s` | `+32.69%` | `better` |
| `segment-index-range-scan:boundedScan` | `23.703 us/op` | `28.235 us/op` | `+19.12%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1866.270 us/op` | `2113.465 us/op` | `+13.25%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3546.753 us/op` | `4096.906 us/op` | `+15.51%` | `better` |
| `segment-merge-sequential:mergeSequential` | `300.397 us/op` | `325.381 us/op` | `+8.32%` | `better` |
