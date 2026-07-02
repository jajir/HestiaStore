# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `80.386 ms/op` | `86.971 ms/op` | `+8.19%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.432 ms/op` | `60.483 ms/op` | `+9.11%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.422 ms/op` | `62.009 ms/op` | `-2.23%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.137 ms/op` | `38.260 ms/op` | `-2.24%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.528 ms/op` | `25.565 ms/op` | `+0.15%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.687 ms/op` | `31.392 ms/op` | `+2.30%` | `neutral` |
