# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `89.633 ms/op` | `91.920 ms/op` | `+2.55%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `59.610 ms/op` | `68.425 ms/op` | `+14.79%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.841 ms/op` | `78.017 ms/op` | `+13.33%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.059 ms/op` | `41.856 ms/op` | `+4.49%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.057 ms/op` | `26.701 ms/op` | `+2.47%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.888 ms/op` | `31.872 ms/op` | `-0.05%` | `neutral` |
