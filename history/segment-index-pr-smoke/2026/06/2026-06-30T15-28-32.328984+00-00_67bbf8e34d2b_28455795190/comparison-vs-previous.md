# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2354562.374 ops/s` | `2184314.846 ops/s` | `-7.23%` | `worse` |
| `segment-index-get-live:getMissSync` | `2023107.225 ops/s` | `2079095.567 ops/s` | `+2.77%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1977496.492 ops/s` | `1841172.534 ops/s` | `-6.89%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2296013.183 ops/s` | `2115075.326 ops/s` | `-7.88%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2146418.744 ops/s` | `2171899.366 ops/s` | `+1.19%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1105193.744 ops/s` | `1112819.589 ops/s` | `+0.69%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `443551.012 ops/s` | `413583.883 ops/s` | `-6.76%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `281955.170 ops/s` | `244294.056 ops/s` | `-13.36%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161595.843 ops/s` | `169289.827 ops/s` | `+4.76%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `539185.871 ops/s` | `554140.498 ops/s` | `+2.77%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `525326.042 ops/s` | `539978.367 ops/s` | `+2.79%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13859.829 ops/s` | `14162.130 ops/s` | `+2.18%` | `neutral` |
