# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.482 ms/op` | `86.254 ms/op` | `+3.32%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.357 ms/op` | `60.937 ms/op` | `+10.08%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.683 ms/op` | `64.636 ms/op` | `+3.12%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.326 ms/op` | `38.056 ms/op` | `-3.23%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.148 ms/op` | `25.871 ms/op` | `-1.06%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.347 ms/op` | `30.102 ms/op` | `-3.97%` | `warning` |
