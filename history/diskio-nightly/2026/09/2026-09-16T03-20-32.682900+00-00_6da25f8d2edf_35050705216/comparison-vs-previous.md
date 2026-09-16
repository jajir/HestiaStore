# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `74.624 ms/op` | `83.588 ms/op` | `+12.01%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `63.033 ms/op` | `64.600 ms/op` | `+2.49%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `64.694 ms/op` | `67.532 ms/op` | `+4.39%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `50.977 ms/op` | `39.293 ms/op` | `-22.92%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `53.406 ms/op` | `26.343 ms/op` | `-50.67%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `53.314 ms/op` | `31.109 ms/op` | `-41.65%` | `worse` |
