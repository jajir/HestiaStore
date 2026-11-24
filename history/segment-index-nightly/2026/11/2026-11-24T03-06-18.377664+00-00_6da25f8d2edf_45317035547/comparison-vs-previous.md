# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6452552.904 ops/s` | `6467945.393 ops/s` | `+0.24%` | `neutral` |
| `segment-index-get-live:getMissSync` | `6204044.132 ops/s` | `5911305.698 ops/s` | `-4.72%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `470707.901 ops/s` | `482060.631 ops/s` | `+2.41%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `6449652.879 ops/s` | `6404477.378 ops/s` | `-0.70%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `5758253.604 ops/s` | `5373193.501 ops/s` | `-6.69%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `6546712.691 ops/s` | `5986682.646 ops/s` | `-8.55%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4613941.089 ops/s` | `5568704.060 ops/s` | `+20.69%` | `better` |
| `segment-index-get-persisted:getMissSync` | `6513388.050 ops/s` | `6457584.214 ops/s` | `-0.86%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `5412341.654 ops/s` | `5867022.484 ops/s` | `+8.40%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3239430.234 ops/s` | `3191908.164 ops/s` | `-1.47%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `201.513 ms/op` | `195.782 ms/op` | `-2.84%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `213.773 ms/op` | `206.731 ms/op` | `-3.29%` | `warning` |
| `segment-index-lifecycle:openExisting` | `202.133 ms/op` | `191.388 ms/op` | `-5.32%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `707723.324 ops/s` | `738533.196 ops/s` | `+4.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `417893.555 ops/s` | `446727.410 ops/s` | `+6.90%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `289829.769 ops/s` | `291805.786 ops/s` | `+0.68%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1778419.210 ops/s` | `2165652.775 ops/s` | `+21.77%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1729985.154 ops/s` | `2116402.907 ops/s` | `+22.34%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `48434.056 ops/s` | `49249.868 ops/s` | `+1.68%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8327.596 ops/s` | `6909.499 ops/s` | `-17.03%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7878.455 ops/s` | `7579.400 ops/s` | `-3.80%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3812.864 ops/s` | `3483.579 ops/s` | `-8.64%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3733.570 ops/s` | `3244.462 ops/s` | `-13.10%` | `worse` |
| `segment-index-range-scan:boundedScan` | `18.022 us/op` | `18.757 us/op` | `+4.08%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1171.262 us/op` | `1190.919 us/op` | `+1.68%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `2317.465 us/op` | `2356.659 us/op` | `+1.69%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `182.901 us/op` | `181.763 us/op` | `-0.62%` | `neutral` |
