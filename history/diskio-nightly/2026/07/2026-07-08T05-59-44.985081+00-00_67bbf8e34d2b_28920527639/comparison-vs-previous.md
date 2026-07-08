# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.469 ms/op` | `87.351 ms/op` | `+4.65%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `57.890 ms/op` | `53.273 ms/op` | `-7.98%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `60.701 ms/op` | `62.694 ms/op` | `+3.28%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.019 ms/op` | `38.553 ms/op` | `-1.19%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.490 ms/op` | `25.620 ms/op` | `+0.51%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.751 ms/op` | `30.666 ms/op` | `-0.28%` | `neutral` |
