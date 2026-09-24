# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `86.862 ms/op` | `92.741 ms/op` | `+6.77%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `53.078 ms/op` | `62.078 ms/op` | `+16.96%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.639 ms/op` | `67.304 ms/op` | `+7.45%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.120 ms/op` | `40.366 ms/op` | `+3.18%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.526 ms/op` | `25.906 ms/op` | `+1.49%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.536 ms/op` | `31.636 ms/op` | `+3.60%` | `better` |
