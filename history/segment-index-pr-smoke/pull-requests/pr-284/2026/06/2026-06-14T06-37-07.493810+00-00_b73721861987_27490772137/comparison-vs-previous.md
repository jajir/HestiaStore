# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `2f845f9e3bbef0938fce783a33e53a259317ea2a`
- Candidate SHA: `b73721861987383934aba1579f4747242c21c76f`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2231657.288 ops/s` | `2429951.667 ops/s` | `+8.89%` | `better` |
| `segment-index-get-live:getMissSync` | `2175192.008 ops/s` | `2068011.659 ops/s` | `-4.93%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1885114.829 ops/s` | `1631227.159 ops/s` | `-13.47%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2136786.662 ops/s` | `2222281.195 ops/s` | `+4.00%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1917498.897 ops/s` | `1950777.158 ops/s` | `+1.74%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1140340.211 ops/s` | `1127660.176 ops/s` | `-1.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `288946.754 ops/s` | `275666.831 ops/s` | `-4.60%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `145970.745 ops/s` | `135538.453 ops/s` | `-7.15%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `142976.009 ops/s` | `140128.378 ops/s` | `-1.99%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `164622.555 ops/s` | `179549.235 ops/s` | `+9.07%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `150352.330 ops/s` | `164848.766 ops/s` | `+9.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14270.226 ops/s` | `14700.469 ops/s` | `+3.01%` | `better` |
