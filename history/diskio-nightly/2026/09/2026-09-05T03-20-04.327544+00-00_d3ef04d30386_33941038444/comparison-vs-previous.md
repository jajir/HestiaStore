# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.422 ms/op` | `83.632 ms/op` | `+0.25%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `62.861 ms/op` | `54.898 ms/op` | `-12.67%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.530 ms/op` | `62.899 ms/op` | `-0.99%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.111 ms/op` | `38.510 ms/op` | `+1.05%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.474 ms/op` | `26.119 ms/op` | `+2.53%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.789 ms/op` | `30.571 ms/op` | `-0.71%` | `neutral` |
