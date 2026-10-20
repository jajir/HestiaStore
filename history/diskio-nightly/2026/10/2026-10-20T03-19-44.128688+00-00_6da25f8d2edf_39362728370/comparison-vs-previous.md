# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.677 ms/op` | `83.302 ms/op` | `-0.45%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `53.067 ms/op` | `56.510 ms/op` | `+6.49%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.076 ms/op` | `64.008 ms/op` | `+1.48%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.759 ms/op` | `39.694 ms/op` | `+2.41%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.851 ms/op` | `27.124 ms/op` | `+1.02%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.133 ms/op` | `31.570 ms/op` | `+1.40%` | `neutral` |
