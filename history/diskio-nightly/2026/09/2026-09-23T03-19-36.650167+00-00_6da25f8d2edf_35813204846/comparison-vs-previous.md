# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.966 ms/op` | `86.862 ms/op` | `+4.70%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `53.183 ms/op` | `53.078 ms/op` | `-0.20%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `65.859 ms/op` | `62.639 ms/op` | `-4.89%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.132 ms/op` | `39.120 ms/op` | `+2.59%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.516 ms/op` | `25.526 ms/op` | `+0.04%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.064 ms/op` | `30.536 ms/op` | `-1.70%` | `neutral` |
