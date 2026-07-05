# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.993 ms/op` | `81.573 ms/op` | `-2.88%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `54.991 ms/op` | `69.005 ms/op` | `+25.48%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.098 ms/op` | `80.290 ms/op` | `+29.30%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.173 ms/op` | `39.502 ms/op` | `+0.84%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.082 ms/op` | `34.491 ms/op` | `+32.24%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.648 ms/op` | `35.403 ms/op` | `+11.86%` | `better` |
