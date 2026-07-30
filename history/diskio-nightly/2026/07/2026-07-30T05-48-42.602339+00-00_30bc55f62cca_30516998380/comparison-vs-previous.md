# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `30bc55f62cca333dbbb24dd3a1d598ed4e18153b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.676 ms/op` | `84.285 ms/op` | `-0.46%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `60.361 ms/op` | `70.039 ms/op` | `+16.03%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.955 ms/op` | `75.410 ms/op` | `+19.78%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.603 ms/op` | `60.680 ms/op` | `+57.19%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.037 ms/op` | `60.348 ms/op` | `+131.78%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.776 ms/op` | `56.132 ms/op` | `+82.39%` | `better` |
