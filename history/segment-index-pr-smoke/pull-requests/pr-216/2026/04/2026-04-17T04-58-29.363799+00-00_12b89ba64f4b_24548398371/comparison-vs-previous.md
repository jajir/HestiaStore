# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `51724b114025e9af1cae85d0e87d4678c8b87310`
- Candidate SHA: `12b89ba64f4bca39f5521e18ca949e04aa583e4f`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3633036.553 ops/s` | `3431333.698 ops/s` | `-5.55%` | `warning` |
| `segment-index-get-live:getMissSync` | `4066344.412 ops/s` | `4071059.775 ops/s` | `+0.12%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `80.544 ops/s` | `90.921 ops/s` | `+12.88%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3938749.230 ops/s` | `3507617.945 ops/s` | `-10.95%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `121858.452 ops/s` | `112409.667 ops/s` | `-7.75%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3805771.927 ops/s` | `3958070.866 ops/s` | `+4.00%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2236263.137 ops/s` | `2262124.838 ops/s` | `+1.16%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1451535.514 ops/s` | `1307736.303 ops/s` | `-9.91%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `345984.822 ops/s` | `342651.067 ops/s` | `-0.96%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `198081.543 ops/s` | `188515.582 ops/s` | `-4.83%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `147903.279 ops/s` | `154135.485 ops/s` | `+4.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `15939.019 ops/s` | `9458.207 ops/s` | `-40.66%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `360.191 ops/s` | `2698.115 ops/s` | `+649.08%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15578.828 ops/s` | `6760.092 ops/s` | `-56.61%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2459.396 ops/s` | `2489.840 ops/s` | `+1.24%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2465.976 ops/s` | `2373.643 ops/s` | `-3.74%` | `warning` |
