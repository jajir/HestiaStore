# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `68.569 ms/op` | `67.448 ms/op` | `-1.63%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `48.311 ms/op` | `45.828 ms/op` | `-5.14%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `52.681 ms/op` | `50.498 ms/op` | `-4.14%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `36.968 ms/op` | `36.368 ms/op` | `-1.62%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `36.762 ms/op` | `37.384 ms/op` | `+1.69%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `36.032 ms/op` | `35.043 ms/op` | `-2.74%` | `neutral` |
