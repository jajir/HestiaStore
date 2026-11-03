# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.028 ms/op` | `89.266 ms/op` | `+7.51%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.865 ms/op` | `58.360 ms/op` | `+2.63%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.110 ms/op` | `64.033 ms/op` | `+4.78%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `37.581 ms/op` | `39.528 ms/op` | `+5.18%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.509 ms/op` | `27.051 ms/op` | `+2.05%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.581 ms/op` | `31.638 ms/op` | `+3.45%` | `better` |
