# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.117 ms/op` | `73.825 ms/op` | `-11.18%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `71.197 ms/op` | `48.772 ms/op` | `-31.50%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `70.723 ms/op` | `54.862 ms/op` | `-22.43%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `53.854 ms/op` | `63.393 ms/op` | `+17.71%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `55.348 ms/op` | `60.754 ms/op` | `+9.77%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `59.562 ms/op` | `57.538 ms/op` | `-3.40%` | `warning` |
