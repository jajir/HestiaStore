# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `91.310 ms/op` | `68.224 ms/op` | `-25.28%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `61.388 ms/op` | `57.548 ms/op` | `-6.26%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `67.656 ms/op` | `58.703 ms/op` | `-13.23%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `57.361 ms/op` | `30.732 ms/op` | `-46.42%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `34.796 ms/op` | `45.073 ms/op` | `+29.54%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `37.415 ms/op` | `34.498 ms/op` | `-7.80%` | `worse` |
