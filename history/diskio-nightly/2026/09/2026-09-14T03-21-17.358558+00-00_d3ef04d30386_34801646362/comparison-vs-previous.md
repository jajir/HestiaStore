# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `89.535 ms/op` | `93.250 ms/op` | `+4.15%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `68.859 ms/op` | `64.685 ms/op` | `-6.06%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `66.558 ms/op` | `68.639 ms/op` | `+3.13%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.324 ms/op` | `40.218 ms/op` | `-0.26%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.547 ms/op` | `25.947 ms/op` | `-2.26%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.306 ms/op` | `31.675 ms/op` | `-1.95%` | `neutral` |
