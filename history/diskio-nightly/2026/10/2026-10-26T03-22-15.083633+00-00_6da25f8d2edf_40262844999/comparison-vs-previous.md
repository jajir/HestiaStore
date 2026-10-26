# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `81.924 ms/op` | `68.569 ms/op` | `-16.30%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `68.168 ms/op` | `48.311 ms/op` | `-29.13%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `69.102 ms/op` | `52.681 ms/op` | `-23.76%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.980 ms/op` | `36.968 ms/op` | `-7.53%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `46.457 ms/op` | `36.762 ms/op` | `-20.87%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `37.439 ms/op` | `36.032 ms/op` | `-3.76%` | `warning` |
