# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `87.351 ms/op` | `84.550 ms/op` | `-3.21%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `53.273 ms/op` | `68.541 ms/op` | `+28.66%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.694 ms/op` | `72.678 ms/op` | `+15.93%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.553 ms/op` | `68.955 ms/op` | `+78.86%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.620 ms/op` | `67.970 ms/op` | `+165.30%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.666 ms/op` | `74.403 ms/op` | `+142.62%` | `better` |
