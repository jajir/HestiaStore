# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `86.254 ms/op` | `97.746 ms/op` | `+13.32%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `60.937 ms/op` | `59.875 ms/op` | `-1.74%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `64.636 ms/op` | `70.385 ms/op` | `+8.89%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.056 ms/op` | `41.814 ms/op` | `+9.87%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.871 ms/op` | `26.073 ms/op` | `+0.78%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.102 ms/op` | `32.398 ms/op` | `+7.63%` | `better` |
