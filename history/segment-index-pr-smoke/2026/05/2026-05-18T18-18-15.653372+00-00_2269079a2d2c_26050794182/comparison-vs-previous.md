# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8a07667173e70d5b86a739f309a23e309b096fe7`
- Candidate SHA: `2269079a2d2cb8a4ff9d6120a27b647ace492203`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2288815.183 ops/s` | `2362296.308 ops/s` | `+3.21%` | `better` |
| `segment-index-get-live:getMissSync` | `3870423.184 ops/s` | `4053978.881 ops/s` | `+4.74%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `12850.929 ops/s` | `12812.868 ops/s` | `-0.30%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3699752.269 ops/s` | `3936142.774 ops/s` | `+6.39%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1786744.514 ops/s` | `1850563.390 ops/s` | `+3.57%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3842629.958 ops/s` | `3818362.061 ops/s` | `-0.63%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1761663.786 ops/s` | `1676570.481 ops/s` | `-4.83%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `971639.679 ops/s` | `1022751.670 ops/s` | `+5.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `278409.655 ops/s` | `281605.595 ops/s` | `+1.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `122177.071 ops/s` | `114909.822 ops/s` | `-5.95%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `156232.584 ops/s` | `166695.773 ops/s` | `+6.70%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `44415.876 ops/s` | `51783.591 ops/s` | `+16.59%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `39072.753 ops/s` | `46279.334 ops/s` | `+18.44%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5343.124 ops/s` | `5504.256 ops/s` | `+3.02%` | `better` |
