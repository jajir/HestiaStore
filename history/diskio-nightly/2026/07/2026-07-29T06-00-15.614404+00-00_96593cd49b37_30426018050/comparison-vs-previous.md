# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `90.908 ms/op` | `84.676 ms/op` | `-6.86%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `54.577 ms/op` | `60.361 ms/op` | `+10.60%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `65.251 ms/op` | `62.955 ms/op` | `-3.52%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.124 ms/op` | `38.603 ms/op` | `-1.33%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.833 ms/op` | `26.037 ms/op` | `+0.79%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.082 ms/op` | `30.776 ms/op` | `-0.99%` | `neutral` |
