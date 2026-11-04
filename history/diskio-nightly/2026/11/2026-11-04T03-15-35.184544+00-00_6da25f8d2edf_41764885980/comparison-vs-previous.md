# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `89.266 ms/op` | `84.755 ms/op` | `-5.05%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.360 ms/op` | `57.201 ms/op` | `-1.99%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `64.033 ms/op` | `64.476 ms/op` | `+0.69%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.528 ms/op` | `38.748 ms/op` | `-1.97%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `27.051 ms/op` | `27.103 ms/op` | `+0.19%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.638 ms/op` | `31.451 ms/op` | `-0.59%` | `neutral` |
