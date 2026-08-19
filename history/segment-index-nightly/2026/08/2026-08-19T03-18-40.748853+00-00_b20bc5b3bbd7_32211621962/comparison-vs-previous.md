# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `b20bc5b3bbd7c2ca573d925820e56e7a9db8b1ac`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4674470.695 ops/s` | `5163116.794 ops/s` | `+10.45%` | `better` |
| `segment-index-get-live:getMissSync` | `4455628.011 ops/s` | `4195012.649 ops/s` | `-5.85%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `279079.982 ops/s` | `290966.448 ops/s` | `+4.26%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4778036.069 ops/s` | `4330607.686 ops/s` | `-9.36%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3227231.839 ops/s` | `3600823.431 ops/s` | `+11.58%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4007233.025 ops/s` | `4750572.351 ops/s` | `+18.55%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3031327.524 ops/s` | `3537519.872 ops/s` | `+16.70%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4186753.993 ops/s` | `4512381.069 ops/s` | `+7.78%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4012291.659 ops/s` | `4353589.655 ops/s` | `+8.51%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2168597.225 ops/s` | `2393983.374 ops/s` | `+10.39%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.912 ms/op` | `247.045 ms/op` | `-11.11%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `301.716 ms/op` | `269.183 ms/op` | `-10.78%` | `worse` |
| `segment-index-lifecycle:openExisting` | `274.955 ms/op` | `242.828 ms/op` | `-11.68%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `537663.749 ops/s` | `535495.697 ops/s` | `-0.40%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `281336.741 ops/s` | `258873.143 ops/s` | `-7.98%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `256327.008 ops/s` | `276622.554 ops/s` | `+7.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1242239.356 ops/s` | `1247421.176 ops/s` | `+0.42%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1225710.773 ops/s` | `1227178.419 ops/s` | `+0.12%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16528.584 ops/s` | `20242.757 ops/s` | `+22.47%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8117.824 ops/s` | `6999.325 ops/s` | `-13.78%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8244.807 ops/s` | `7181.265 ops/s` | `-12.90%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3187.318 ops/s` | `2444.119 ops/s` | `-23.32%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2987.180 ops/s` | `2406.481 ops/s` | `-19.44%` | `worse` |
| `segment-index-range-scan:boundedScan` | `32.218 us/op` | `41.145 us/op` | `+27.71%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2106.164 us/op` | `1996.329 us/op` | `-5.21%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4179.988 us/op` | `3922.354 us/op` | `-6.16%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `358.906 us/op` | `332.667 us/op` | `-7.31%` | `worse` |
