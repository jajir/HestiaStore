# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `b20bc5b3bbd7c2ca573d925820e56e7a9db8b1ac`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5163116.794 ops/s` | `4256750.029 ops/s` | `-17.55%` | `worse` |
| `segment-index-get-live:getMissSync` | `4195012.649 ops/s` | `4428066.556 ops/s` | `+5.56%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `290966.448 ops/s` | `285340.813 ops/s` | `-1.93%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4330607.686 ops/s` | `4255511.935 ops/s` | `-1.73%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3600823.431 ops/s` | `3526148.200 ops/s` | `-2.07%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4750572.351 ops/s` | `3957283.144 ops/s` | `-16.70%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3537519.872 ops/s` | `3424899.241 ops/s` | `-3.18%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4512381.069 ops/s` | `4539923.485 ops/s` | `+0.61%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4353589.655 ops/s` | `4056473.857 ops/s` | `-6.82%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2393983.374 ops/s` | `2119230.623 ops/s` | `-11.48%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.045 ms/op` | `276.381 ms/op` | `+11.87%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `269.183 ms/op` | `299.359 ms/op` | `+11.21%` | `better` |
| `segment-index-lifecycle:openExisting` | `242.828 ms/op` | `271.413 ms/op` | `+11.77%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `535495.697 ops/s` | `544853.637 ops/s` | `+1.75%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `258873.143 ops/s` | `262989.613 ops/s` | `+1.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `276622.554 ops/s` | `281864.024 ops/s` | `+1.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1247421.176 ops/s` | `1261037.940 ops/s` | `+1.09%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1227178.419 ops/s` | `1239694.790 ops/s` | `+1.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20242.757 ops/s` | `21343.151 ops/s` | `+5.44%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6999.325 ops/s` | `8290.534 ops/s` | `+18.45%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7181.265 ops/s` | `8185.416 ops/s` | `+13.98%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2444.119 ops/s` | `3255.188 ops/s` | `+33.18%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2406.481 ops/s` | `3159.621 ops/s` | `+31.30%` | `better` |
| `segment-index-range-scan:boundedScan` | `41.145 us/op` | `31.053 us/op` | `-24.53%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1996.329 us/op` | `2128.254 us/op` | `+6.61%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3922.354 us/op` | `4205.713 us/op` | `+7.22%` | `better` |
| `segment-merge-sequential:mergeSequential` | `332.667 us/op` | `358.481 us/op` | `+7.76%` | `better` |
