# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.023 ms/op` | `83.117 ms/op` | `+0.11%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.409 ms/op` | `71.197 ms/op` | `+28.49%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.098 ms/op` | `70.723 ms/op` | `+13.89%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.595 ms/op` | `53.854 ms/op` | `+39.54%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.644 ms/op` | `55.348 ms/op` | `+107.73%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.648 ms/op` | `59.562 ms/op` | `+88.20%` | `better` |
