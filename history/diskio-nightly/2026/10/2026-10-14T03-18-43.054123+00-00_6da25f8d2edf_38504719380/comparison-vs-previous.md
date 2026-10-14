# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.178 ms/op` | `71.608 ms/op` | `-13.91%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `57.957 ms/op` | `47.658 ms/op` | `-17.77%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.485 ms/op` | `50.829 ms/op` | `-25.78%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.670 ms/op` | `36.114 ms/op` | `-6.61%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.920 ms/op` | `34.593 ms/op` | `+28.50%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.701 ms/op` | `33.869 ms/op` | `+6.84%` | `better` |
