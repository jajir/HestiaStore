# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.822 ms/op` | `81.114 ms/op` | `-4.37%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `60.310 ms/op` | `55.067 ms/op` | `-8.69%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `60.542 ms/op` | `61.295 ms/op` | `+1.24%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.348 ms/op` | `40.255 ms/op` | `+2.30%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.710 ms/op` | `27.552 ms/op` | `+3.15%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.519 ms/op` | `31.838 ms/op` | `+1.01%` | `neutral` |
