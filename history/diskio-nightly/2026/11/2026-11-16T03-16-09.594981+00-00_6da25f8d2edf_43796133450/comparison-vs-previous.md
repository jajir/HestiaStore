# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `67.866 ms/op` | `91.310 ms/op` | `+34.55%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `57.461 ms/op` | `61.388 ms/op` | `+6.83%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `60.291 ms/op` | `67.656 ms/op` | `+12.22%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `31.008 ms/op` | `57.361 ms/op` | `+84.99%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `39.067 ms/op` | `34.796 ms/op` | `-10.93%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `33.051 ms/op` | `37.415 ms/op` | `+13.20%` | `better` |
