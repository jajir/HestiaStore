# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67ecf099e47860f6b644e6e59ef58bc83f0c7dda`
- Candidate SHA: `6885ee00040024ba5bc2fc1f0365778414d7b12d`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.490 ops/s` | `83.903 ops/s` | `-11.20%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.183 ops/s` | `99.230 ops/s` | `+6.49%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `244391.453 ops/s` | `243255.237 ops/s` | `-0.46%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4530689.079 ops/s` | `2437548.573 ops/s` | `-46.20%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `233743.255 ops/s` | `242777.644 ops/s` | `+3.87%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3583154.029 ops/s` | `2954289.044 ops/s` | `-17.55%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `239787.902 ops/s` | `219411.327 ops/s` | `-8.50%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `5106187.796 ops/s` | `2469303.631 ops/s` | `-51.64%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `73128.651 ops/s` | `70177.302 ops/s` | `-4.04%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `103009.233 ops/s` | `104995.575 ops/s` | `+1.93%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `223959.043 ops/s` | `223485.668 ops/s` | `-0.21%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4840763.562 ops/s` | `2495575.521 ops/s` | `-48.45%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `-` | `1994768.057 ops/s` | `-` | `new` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `-` | `1150147.832 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `432270.806 ops/s` | `457036.784 ops/s` | `+5.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `426368.651 ops/s` | `451654.220 ops/s` | `+5.93%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5902.155 ops/s` | `5382.564 ops/s` | `-8.80%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `209985.530 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `207516.216 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2469.314 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `-` | `2928.631 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:deleteSync` | `-` | `3254.227 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:putAsyncJoin` | `-` | `2844.211 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:putSync` | `-` | `3192.822 ops/s` | `-` | `new` |
