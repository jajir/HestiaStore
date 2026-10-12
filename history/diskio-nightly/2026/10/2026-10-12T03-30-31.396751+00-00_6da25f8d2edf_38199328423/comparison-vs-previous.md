# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `73.739 ms/op` | `71.832 ms/op` | `-2.59%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `50.535 ms/op` | `46.254 ms/op` | `-8.47%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `53.553 ms/op` | `52.678 ms/op` | `-1.63%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.998 ms/op` | `36.893 ms/op` | `-12.16%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `32.421 ms/op` | `33.874 ms/op` | `+4.48%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `34.313 ms/op` | `34.327 ms/op` | `+0.04%` | `neutral` |
