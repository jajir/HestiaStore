# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.588 ms/op` | `83.604 ms/op` | `+0.02%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `64.600 ms/op` | `56.660 ms/op` | `-12.29%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `67.532 ms/op` | `62.967 ms/op` | `-6.76%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.293 ms/op` | `39.375 ms/op` | `+0.21%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.343 ms/op` | `26.006 ms/op` | `-1.28%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.109 ms/op` | `31.016 ms/op` | `-0.30%` | `neutral` |
