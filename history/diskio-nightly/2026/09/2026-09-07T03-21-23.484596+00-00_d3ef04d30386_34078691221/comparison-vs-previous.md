# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.473 ms/op` | `82.457 ms/op` | `-2.39%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `62.421 ms/op` | `56.482 ms/op` | `-9.51%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.528 ms/op` | `62.374 ms/op` | `+1.37%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `37.699 ms/op` | `38.274 ms/op` | `+1.53%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.388 ms/op` | `25.530 ms/op` | `+0.56%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.233 ms/op` | `30.373 ms/op` | `+0.46%` | `neutral` |
