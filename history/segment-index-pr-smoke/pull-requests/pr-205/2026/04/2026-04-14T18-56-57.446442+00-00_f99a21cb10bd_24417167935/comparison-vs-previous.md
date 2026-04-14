# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `435bb29609f064befe1e1c40df2c191ede262526`
- Candidate SHA: `f99a21cb10bd46a6fdd60b330008434a8a859efb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3298297.385 ops/s` | `3473490.042 ops/s` | `+5.31%` | `better` |
| `segment-index-get-live:getMissSync` | `3800325.451 ops/s` | `3753814.927 ops/s` | `-1.22%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.580 ops/s` | `160.771 ops/s` | `+77.49%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3511913.035 ops/s` | `3852139.178 ops/s` | `+9.69%` | `better` |
| `segment-index-get-persisted:getHitSync` | `120653.701 ops/s` | `106795.716 ops/s` | `-11.49%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3717581.594 ops/s` | `3802623.929 ops/s` | `+2.29%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2686765.353 ops/s` | `2423924.900 ops/s` | `-9.78%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1516468.968 ops/s` | `1404395.880 ops/s` | `-7.39%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `376436.389 ops/s` | `364341.707 ops/s` | `-3.21%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `189010.213 ops/s` | `232730.235 ops/s` | `+23.13%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `187426.177 ops/s` | `131611.473 ops/s` | `-29.78%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `34409.351 ops/s` | `23159.218 ops/s` | `-32.69%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `27440.975 ops/s` | `15180.932 ops/s` | `-44.68%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `6968.376 ops/s` | `7978.286 ops/s` | `+14.49%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3714.204 ops/s` | `3552.328 ops/s` | `-4.36%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `3526.648 ops/s` | `3419.813 ops/s` | `-3.03%` | `warning` |
