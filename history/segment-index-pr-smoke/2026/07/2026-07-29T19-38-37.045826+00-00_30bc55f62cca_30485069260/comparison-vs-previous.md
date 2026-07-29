# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `30bc55f62cca333dbbb24dd3a1d598ed4e18153b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2175809.756 ops/s` | `2353346.313 ops/s` | `+8.16%` | `better` |
| `segment-index-get-live:getMissSync` | `2339581.055 ops/s` | `2117047.635 ops/s` | `-9.51%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2027486.948 ops/s` | `1980443.386 ops/s` | `-2.32%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2009759.359 ops/s` | `2169804.997 ops/s` | `+7.96%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2131503.161 ops/s` | `2157191.359 ops/s` | `+1.21%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1112462.064 ops/s` | `1110239.014 ops/s` | `-0.20%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `475449.831 ops/s` | `464106.368 ops/s` | `-2.39%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `304120.254 ops/s` | `282171.823 ops/s` | `-7.22%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `171329.577 ops/s` | `181934.545 ops/s` | `+6.19%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `559154.977 ops/s` | `606572.532 ops/s` | `+8.48%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `544572.760 ops/s` | `591517.034 ops/s` | `+8.62%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14582.217 ops/s` | `15055.498 ops/s` | `+3.25%` | `better` |
