# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2936211.793 ops/s` | `5680824.953 ops/s` | `+93.47%` | `better` |
| `segment-index-get-live:getMissSync` | `2525102.012 ops/s` | `5410111.787 ops/s` | `+114.25%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `380770.221 ops/s` | `403418.935 ops/s` | `+5.95%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3016051.460 ops/s` | `5756577.563 ops/s` | `+90.86%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2445659.943 ops/s` | `4527335.623 ops/s` | `+85.12%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2669505.729 ops/s` | `5644613.980 ops/s` | `+111.45%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2550997.271 ops/s` | `4337979.512 ops/s` | `+70.05%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2893036.087 ops/s` | `5729598.966 ops/s` | `+98.05%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2639072.958 ops/s` | `5073653.976 ops/s` | `+92.25%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1401533.611 ops/s` | `2664522.562 ops/s` | `+90.11%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `99.504 ms/op` | `212.079 ms/op` | `+113.14%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `109.457 ms/op` | `232.009 ms/op` | `+111.96%` | `better` |
| `segment-index-lifecycle:openExisting` | `98.216 ms/op` | `213.256 ms/op` | `+117.13%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `615988.152 ops/s` | `660446.449 ops/s` | `+7.22%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `315827.735 ops/s` | `347215.291 ops/s` | `+9.94%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `300160.416 ops/s` | `313231.158 ops/s` | `+4.35%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1485674.916 ops/s` | `1765642.347 ops/s` | `+18.84%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1457498.294 ops/s` | `1741715.013 ops/s` | `+19.50%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `28176.622 ops/s` | `23927.334 ops/s` | `-15.08%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `828.459 ops/s` | `3827.514 ops/s` | `+362.00%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `729.801 ops/s` | `4814.039 ops/s` | `+559.64%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `332.128 ops/s` | `2224.433 ops/s` | `+569.75%` | `better` |
| `segment-index-persisted-mutation:putSync` | `875.992 ops/s` | `1345.591 ops/s` | `+53.61%` | `better` |
| `segment-index-range-scan:boundedScan` | `18.330 us/op` | `23.250 us/op` | `+26.84%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1447.502 us/op` | `1566.085 us/op` | `+8.19%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2791.527 us/op` | `2997.187 us/op` | `+7.37%` | `better` |
| `segment-merge-sequential:mergeSequential` | `220.702 us/op` | `280.129 us/op` | `+26.93%` | `better` |
