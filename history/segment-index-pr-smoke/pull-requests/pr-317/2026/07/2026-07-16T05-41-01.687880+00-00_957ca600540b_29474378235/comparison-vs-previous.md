# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Candidate SHA: `957ca600540b53220809c6159850ededfb429366`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2128615.488 ops/s` | `2125602.431 ops/s` | `-0.14%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1974115.093 ops/s` | `2149488.302 ops/s` | `+8.88%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2178382.682 ops/s` | `1954007.774 ops/s` | `-10.30%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `1996652.639 ops/s` | `2137566.949 ops/s` | `+7.06%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2196538.057 ops/s` | `2137287.023 ops/s` | `-2.70%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1093203.200 ops/s` | `1124595.287 ops/s` | `+2.87%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `472297.189 ops/s` | `470347.270 ops/s` | `-0.41%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `306890.975 ops/s` | `294231.021 ops/s` | `-4.13%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165406.214 ops/s` | `176116.248 ops/s` | `+6.47%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `674707.341 ops/s` | `611054.852 ops/s` | `-9.43%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `658505.033 ops/s` | `595885.707 ops/s` | `-9.51%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16202.308 ops/s` | `15169.145 ops/s` | `-6.38%` | `warning` |
