# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `73.121 ms/op` | `84.138 ms/op` | `+15.07%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `49.751 ms/op` | `56.949 ms/op` | `+14.47%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `55.795 ms/op` | `62.601 ms/op` | `+12.20%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `67.625 ms/op` | `39.759 ms/op` | `-41.21%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `39.612 ms/op` | `26.989 ms/op` | `-31.87%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `54.379 ms/op` | `31.406 ms/op` | `-42.25%` | `worse` |
