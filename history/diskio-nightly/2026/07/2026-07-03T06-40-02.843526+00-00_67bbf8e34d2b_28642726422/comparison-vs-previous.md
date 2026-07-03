# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `86.971 ms/op` | `84.496 ms/op` | `-2.85%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `60.483 ms/op` | `55.896 ms/op` | `-7.58%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.009 ms/op` | `63.462 ms/op` | `+2.34%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.260 ms/op` | `39.262 ms/op` | `+2.62%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.565 ms/op` | `26.531 ms/op` | `+3.78%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.392 ms/op` | `31.850 ms/op` | `+1.46%` | `neutral` |
