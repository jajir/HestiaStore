# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `71.608 ms/op` | `84.330 ms/op` | `+17.77%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `47.658 ms/op` | `71.736 ms/op` | `+50.52%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `50.829 ms/op` | `72.660 ms/op` | `+42.95%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `36.114 ms/op` | `40.649 ms/op` | `+12.56%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `34.593 ms/op` | `34.825 ms/op` | `+0.67%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `33.869 ms/op` | `38.622 ms/op` | `+14.03%` | `better` |
