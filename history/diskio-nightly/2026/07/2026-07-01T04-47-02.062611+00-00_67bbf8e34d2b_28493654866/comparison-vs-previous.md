# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `92.148 ms/op` | `80.386 ms/op` | `-12.76%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.415 ms/op` | `55.432 ms/op` | `+0.03%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `65.324 ms/op` | `63.422 ms/op` | `-2.91%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `37.486 ms/op` | `39.137 ms/op` | `+4.41%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.619 ms/op` | `25.528 ms/op` | `-0.36%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.522 ms/op` | `30.687 ms/op` | `+0.54%` | `neutral` |
