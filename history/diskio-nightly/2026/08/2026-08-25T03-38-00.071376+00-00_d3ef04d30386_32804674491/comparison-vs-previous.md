# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.783 ms/op` | `83.023 ms/op` | `-0.91%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.487 ms/op` | `55.409 ms/op` | `-1.91%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.050 ms/op` | `62.098 ms/op` | `-1.51%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.508 ms/op` | `38.595 ms/op` | `+0.23%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.560 ms/op` | `26.644 ms/op` | `+4.24%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.374 ms/op` | `31.648 ms/op` | `+4.20%` | `better` |
