# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.457 ms/op` | `84.262 ms/op` | `+2.19%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.482 ms/op` | `56.624 ms/op` | `+0.25%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.374 ms/op` | `61.541 ms/op` | `-1.34%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.274 ms/op` | `38.281 ms/op` | `+0.02%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.530 ms/op` | `25.488 ms/op` | `-0.16%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.373 ms/op` | `30.576 ms/op` | `+0.67%` | `neutral` |
