# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `88.878 ms/op` | `90.803 ms/op` | `+2.16%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.500 ms/op` | `55.342 ms/op` | `-2.05%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.812 ms/op` | `62.243 ms/op` | `-0.91%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `37.994 ms/op` | `39.060 ms/op` | `+2.81%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.539 ms/op` | `25.600 ms/op` | `+0.24%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.526 ms/op` | `30.908 ms/op` | `+1.25%` | `neutral` |
