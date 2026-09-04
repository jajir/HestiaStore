# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `91.920 ms/op` | `83.422 ms/op` | `-9.24%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `68.425 ms/op` | `62.861 ms/op` | `-8.13%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `78.017 ms/op` | `63.530 ms/op` | `-18.57%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.856 ms/op` | `38.111 ms/op` | `-8.95%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.701 ms/op` | `25.474 ms/op` | `-4.59%` | `warning` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.872 ms/op` | `30.789 ms/op` | `-3.40%` | `warning` |
