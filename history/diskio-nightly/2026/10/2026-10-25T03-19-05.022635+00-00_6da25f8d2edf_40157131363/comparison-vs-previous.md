# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `73.691 ms/op` | `81.924 ms/op` | `+11.17%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `50.339 ms/op` | `68.168 ms/op` | `+35.42%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `54.958 ms/op` | `69.102 ms/op` | `+25.74%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `32.564 ms/op` | `39.980 ms/op` | `+22.77%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `31.877 ms/op` | `46.457 ms/op` | `+45.74%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `33.119 ms/op` | `37.439 ms/op` | `+13.04%` | `better` |
