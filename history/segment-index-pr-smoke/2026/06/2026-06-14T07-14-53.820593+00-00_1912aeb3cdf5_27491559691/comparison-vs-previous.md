# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `2f845f9e3bbef0938fce783a33e53a259317ea2a`
- Candidate SHA: `1912aeb3cdf5d5e5748b841b244c8640aad43d62`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2385092.108 ops/s` | `2233438.295 ops/s` | `-6.36%` | `warning` |
| `segment-index-get-live:getMissSync` | `2198935.312 ops/s` | `2220159.228 ops/s` | `+0.97%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1837385.122 ops/s` | `1897544.756 ops/s` | `+3.27%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2354806.477 ops/s` | `2243854.396 ops/s` | `-4.71%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1860024.999 ops/s` | `1921153.055 ops/s` | `+3.29%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1145043.600 ops/s` | `1115640.991 ops/s` | `-2.57%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `286051.746 ops/s` | `286543.005 ops/s` | `+0.17%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `147296.395 ops/s` | `143487.130 ops/s` | `-2.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `138755.351 ops/s` | `143055.875 ops/s` | `+3.10%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `171888.704 ops/s` | `175947.865 ops/s` | `+2.36%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `158363.041 ops/s` | `161766.429 ops/s` | `+2.15%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13525.663 ops/s` | `14181.437 ops/s` | `+4.85%` | `better` |
