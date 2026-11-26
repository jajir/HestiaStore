# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `81.180 ms/op` | `84.822 ms/op` | `+4.49%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `59.683 ms/op` | `60.310 ms/op` | `+1.05%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.627 ms/op` | `60.542 ms/op` | `-1.76%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.908 ms/op` | `39.348 ms/op` | `+1.13%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.860 ms/op` | `26.710 ms/op` | `-0.56%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.776 ms/op` | `31.519 ms/op` | `-0.81%` | `neutral` |
