# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `71.313 ms/op` | `72.967 ms/op` | `+2.32%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `49.672 ms/op` | `48.346 ms/op` | `-2.67%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `53.708 ms/op` | `57.160 ms/op` | `+6.43%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `37.392 ms/op` | `39.200 ms/op` | `+4.84%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `35.359 ms/op` | `41.622 ms/op` | `+17.71%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `44.551 ms/op` | `35.204 ms/op` | `-20.98%` | `worse` |
