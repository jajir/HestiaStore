# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `75.611 ms/op` | `84.355 ms/op` | `+11.57%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `50.570 ms/op` | `64.371 ms/op` | `+27.29%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `55.407 ms/op` | `61.268 ms/op` | `+10.58%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.693 ms/op` | `38.722 ms/op` | `-2.45%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `36.800 ms/op` | `26.633 ms/op` | `-27.63%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `35.152 ms/op` | `31.335 ms/op` | `-10.86%` | `worse` |
