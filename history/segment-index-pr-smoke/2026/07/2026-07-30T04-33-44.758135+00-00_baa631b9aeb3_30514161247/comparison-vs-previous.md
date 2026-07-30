# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c0ab96dedfed051af6401d8322b0e7c046e10a49`
- Candidate SHA: `baa631b9aeb3dde8a7971d7b3ec854f5fa53ff07`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2530263.942 ops/s` | `5169230.049 ops/s` | `+104.30%` | `better` |
| `segment-index-get-live:getMissSync` | `2739626.310 ops/s` | `4835118.360 ops/s` | `+76.49%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1969403.324 ops/s` | `3451656.172 ops/s` | `+75.26%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2842652.580 ops/s` | `4778859.820 ops/s` | `+68.11%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2073945.683 ops/s` | `3358557.469 ops/s` | `+61.94%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2964942.676 ops/s` | `4780629.071 ops/s` | `+61.24%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2479638.879 ops/s` | `4239451.001 ops/s` | `+70.97%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1343640.151 ops/s` | `2211979.147 ops/s` | `+64.63%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `469205.336 ops/s` | `552384.954 ops/s` | `+17.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `281163.577 ops/s` | `384317.055 ops/s` | `+36.69%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `188041.758 ops/s` | `168067.899 ops/s` | `-10.62%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `794823.360 ops/s` | `742435.014 ops/s` | `-6.59%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `779877.493 ops/s` | `728907.378 ops/s` | `-6.54%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14945.867 ops/s` | `13527.636 ops/s` | `-9.49%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `4257.990 ops/s` | `6681.408 ops/s` | `+56.91%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3839.772 ops/s` | `6538.217 ops/s` | `+70.28%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1973.337 ops/s` | `2763.887 ops/s` | `+40.06%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1827.851 ops/s` | `2640.453 ops/s` | `+44.46%` | `better` |
| `segment-index-range-scan:boundedScan` | `23.703 us/op` | `28.403 us/op` | `+19.83%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1866.270 us/op` | `2128.094 us/op` | `+14.03%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3546.753 us/op` | `4074.849 us/op` | `+14.89%` | `better` |
| `segment-merge-sequential:mergeSequential` | `300.397 us/op` | `325.873 us/op` | `+8.48%` | `better` |
