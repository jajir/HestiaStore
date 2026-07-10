# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.550 ms/op` | `96.365 ms/op` | `+13.98%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `68.541 ms/op` | `59.983 ms/op` | `-12.49%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `72.678 ms/op` | `68.373 ms/op` | `-5.92%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `68.955 ms/op` | `41.496 ms/op` | `-39.82%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `67.970 ms/op` | `26.972 ms/op` | `-60.32%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `74.403 ms/op` | `32.424 ms/op` | `-56.42%` | `worse` |
