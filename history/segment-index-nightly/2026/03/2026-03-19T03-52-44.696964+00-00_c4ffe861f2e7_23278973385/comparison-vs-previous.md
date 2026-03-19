# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `77b6e427d25f87af9ee3bd0d0f697c9458a844d1`
- Candidate SHA: `c4ffe861f2e7080db119fe7908b4271a81fda692`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `70.638 ops/s` | `86.338 ops/s` | `+22.23%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `71.352 ops/s` | `67.898 ops/s` | `-4.84%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172045.350 ops/s` | `175420.096 ops/s` | `+1.96%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `6901663.622 ops/s` | `5843026.612 ops/s` | `-15.34%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `115.706 ops/s` | `122.268 ops/s` | `+5.67%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `116.162 ops/s` | `120.600 ops/s` | `+3.82%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `172893.118 ops/s` | `173659.537 ops/s` | `+0.44%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `6608005.535 ops/s` | `6135707.369 ops/s` | `-7.15%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160145.597 ops/s` | `172547.973 ops/s` | `+7.74%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4999500.682 ops/s` | `4762639.931 ops/s` | `-4.74%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169121.308 ops/s` | `168133.261 ops/s` | `-0.58%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `6597336.641 ops/s` | `7400922.583 ops/s` | `+12.18%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61316.814 ops/s` | `61032.119 ops/s` | `-0.46%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113793.595 ops/s` | `115700.711 ops/s` | `+1.68%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `169733.642 ops/s` | `169561.143 ops/s` | `-0.10%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `7252195.701 ops/s` | `5853216.935 ops/s` | `-19.29%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `533554.449 ops/s` | `532475.753 ops/s` | `-0.20%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `527571.363 ops/s` | `526617.478 ops/s` | `-0.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5983.085 ops/s` | `5858.276 ops/s` | `-2.09%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `273210.844 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `271570.324 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1640.520 ops/s` | `-` | `-` | `removed` |
