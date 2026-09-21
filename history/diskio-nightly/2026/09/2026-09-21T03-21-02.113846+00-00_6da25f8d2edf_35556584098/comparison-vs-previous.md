# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.496 ms/op` | `91.801 ms/op` | `+9.95%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.467 ms/op` | `61.137 ms/op` | `+8.27%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.398 ms/op` | `68.361 ms/op` | `+7.83%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.311 ms/op` | `40.035 ms/op` | `+4.50%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.918 ms/op` | `26.066 ms/op` | `+0.57%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.684 ms/op` | `31.763 ms/op` | `+3.52%` | `better` |
