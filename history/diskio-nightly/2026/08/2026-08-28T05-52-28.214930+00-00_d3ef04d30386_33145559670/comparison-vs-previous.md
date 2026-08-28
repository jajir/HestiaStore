# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `73.825 ms/op` | `72.021 ms/op` | `-2.44%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `48.772 ms/op` | `49.418 ms/op` | `+1.32%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `54.862 ms/op` | `54.972 ms/op` | `+0.20%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `63.393 ms/op` | `57.405 ms/op` | `-9.45%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `60.754 ms/op` | `50.760 ms/op` | `-16.45%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `57.538 ms/op` | `53.480 ms/op` | `-7.05%` | `worse` |
