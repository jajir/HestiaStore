# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `81.573 ms/op` | `83.501 ms/op` | `+2.36%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `69.005 ms/op` | `58.986 ms/op` | `-14.52%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `80.290 ms/op` | `61.600 ms/op` | `-23.28%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.502 ms/op` | `38.662 ms/op` | `-2.13%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `34.491 ms/op` | `25.523 ms/op` | `-26.00%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `35.403 ms/op` | `30.835 ms/op` | `-12.90%` | `worse` |
