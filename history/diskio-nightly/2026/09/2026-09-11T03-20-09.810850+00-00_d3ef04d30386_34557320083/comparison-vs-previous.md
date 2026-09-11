# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.247 ms/op` | `79.910 ms/op` | `-4.01%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.327 ms/op` | `56.636 ms/op` | `+0.55%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.286 ms/op` | `68.202 ms/op` | `+11.28%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.060 ms/op` | `38.543 ms/op` | `-1.32%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.916 ms/op` | `25.376 ms/op` | `-2.08%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.646 ms/op` | `30.491 ms/op` | `-3.65%` | `warning` |
