# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `67.448 ms/op` | `73.121 ms/op` | `+8.41%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `45.828 ms/op` | `49.751 ms/op` | `+8.56%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `50.498 ms/op` | `55.795 ms/op` | `+10.49%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `36.368 ms/op` | `67.625 ms/op` | `+85.95%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `37.384 ms/op` | `39.612 ms/op` | `+5.96%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `35.043 ms/op` | `54.379 ms/op` | `+55.18%` | `better` |
