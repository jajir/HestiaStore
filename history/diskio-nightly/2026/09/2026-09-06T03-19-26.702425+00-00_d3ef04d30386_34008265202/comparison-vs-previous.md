# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.632 ms/op` | `84.473 ms/op` | `+1.01%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `54.898 ms/op` | `62.421 ms/op` | `+13.70%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.899 ms/op` | `61.528 ms/op` | `-2.18%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.510 ms/op` | `37.699 ms/op` | `-2.11%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.119 ms/op` | `25.388 ms/op` | `-2.80%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.571 ms/op` | `30.233 ms/op` | `-1.10%` | `neutral` |
