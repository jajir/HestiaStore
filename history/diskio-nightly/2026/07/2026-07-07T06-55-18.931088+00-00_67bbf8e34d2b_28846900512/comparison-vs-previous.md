# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.501 ms/op` | `83.469 ms/op` | `-0.04%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.986 ms/op` | `57.890 ms/op` | `-1.86%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.600 ms/op` | `60.701 ms/op` | `-1.46%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.662 ms/op` | `39.019 ms/op` | `+0.92%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.523 ms/op` | `25.490 ms/op` | `-0.13%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.835 ms/op` | `30.751 ms/op` | `-0.27%` | `neutral` |
