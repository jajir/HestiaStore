# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.604 ms/op` | `82.646 ms/op` | `-1.15%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.660 ms/op` | `62.729 ms/op` | `+10.71%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.967 ms/op` | `61.947 ms/op` | `-1.62%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.375 ms/op` | `38.343 ms/op` | `-2.62%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.006 ms/op` | `25.484 ms/op` | `-2.01%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.016 ms/op` | `30.123 ms/op` | `-2.88%` | `neutral` |
