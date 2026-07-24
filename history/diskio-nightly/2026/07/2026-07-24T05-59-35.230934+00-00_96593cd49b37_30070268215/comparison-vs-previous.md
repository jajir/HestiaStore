# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `89.237 ms/op` | `96.530 ms/op` | `+8.17%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.064 ms/op` | `59.766 ms/op` | `+6.60%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `60.791 ms/op` | `67.820 ms/op` | `+11.56%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.093 ms/op` | `42.154 ms/op` | `+10.66%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.248 ms/op` | `26.120 ms/op` | `+3.45%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.048 ms/op` | `32.151 ms/op` | `+7.00%` | `better` |
