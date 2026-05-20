# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8cbc93641a416f6c24aa4899be7dd94a028fa6f9`
- Candidate SHA: `24a53ff220604e4a2d02e63b54909f2f91ff0ce1`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2169082.704 ops/s` | `2138736.615 ops/s` | `-1.40%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2067161.585 ops/s` | `1977805.435 ops/s` | `-4.32%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1861109.894 ops/s` | `1628010.803 ops/s` | `-12.52%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `1914508.961 ops/s` | `2060172.769 ops/s` | `+7.61%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2035936.749 ops/s` | `1912127.896 ops/s` | `-6.08%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1098192.170 ops/s` | `1105122.749 ops/s` | `+0.63%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `280012.976 ops/s` | `297062.161 ops/s` | `+6.09%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `133674.459 ops/s` | `135072.445 ops/s` | `+1.05%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `146338.517 ops/s` | `161989.715 ops/s` | `+10.70%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `130571.996 ops/s` | `167165.241 ops/s` | `+28.03%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `113148.959 ops/s` | `153028.187 ops/s` | `+35.24%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17423.037 ops/s` | `14137.054 ops/s` | `-18.86%` | `worse` |
