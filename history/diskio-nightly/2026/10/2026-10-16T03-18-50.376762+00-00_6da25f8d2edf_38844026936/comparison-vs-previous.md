# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.330 ms/op` | `71.313 ms/op` | `-15.44%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `71.736 ms/op` | `49.672 ms/op` | `-30.76%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `72.660 ms/op` | `53.708 ms/op` | `-26.08%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.649 ms/op` | `37.392 ms/op` | `-8.01%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `34.825 ms/op` | `35.359 ms/op` | `+1.53%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `38.622 ms/op` | `44.551 ms/op` | `+15.35%` | `better` |
