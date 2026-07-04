# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.496 ms/op` | `83.993 ms/op` | `-0.60%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.896 ms/op` | `54.991 ms/op` | `-1.62%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.462 ms/op` | `62.098 ms/op` | `-2.15%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.262 ms/op` | `39.173 ms/op` | `-0.22%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.531 ms/op` | `26.082 ms/op` | `-1.69%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.850 ms/op` | `31.648 ms/op` | `-0.63%` | `neutral` |
