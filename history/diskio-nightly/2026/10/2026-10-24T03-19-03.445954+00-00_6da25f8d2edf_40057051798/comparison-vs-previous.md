# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.482 ms/op` | `73.691 ms/op` | `-11.73%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.223 ms/op` | `50.339 ms/op` | `-10.47%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.698 ms/op` | `54.958 ms/op` | `-12.35%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.645 ms/op` | `32.564 ms/op` | `-17.86%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.693 ms/op` | `31.877 ms/op` | `+19.42%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.218 ms/op` | `33.119 ms/op` | `+6.09%` | `better` |
