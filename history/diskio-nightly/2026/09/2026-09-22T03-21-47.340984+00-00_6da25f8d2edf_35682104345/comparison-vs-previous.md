# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `91.801 ms/op` | `82.966 ms/op` | `-9.62%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `61.137 ms/op` | `53.183 ms/op` | `-13.01%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.361 ms/op` | `65.859 ms/op` | `-3.66%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.035 ms/op` | `38.132 ms/op` | `-4.75%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.066 ms/op` | `25.516 ms/op` | `-2.11%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.763 ms/op` | `31.064 ms/op` | `-2.20%` | `neutral` |
