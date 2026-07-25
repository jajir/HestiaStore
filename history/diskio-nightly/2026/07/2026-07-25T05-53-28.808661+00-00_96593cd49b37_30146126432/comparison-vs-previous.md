# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `96.530 ms/op` | `84.010 ms/op` | `-12.97%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `59.766 ms/op` | `55.112 ms/op` | `-7.79%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `67.820 ms/op` | `61.418 ms/op` | `-9.44%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `42.154 ms/op` | `38.615 ms/op` | `-8.39%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.120 ms/op` | `25.592 ms/op` | `-2.02%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.151 ms/op` | `32.783 ms/op` | `+1.96%` | `neutral` |
