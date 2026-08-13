# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.247 ms/op` | `102.961 ms/op` | `+23.68%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `57.457 ms/op` | `59.103 ms/op` | `+2.86%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.009 ms/op` | `65.470 ms/op` | `+3.90%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.871 ms/op` | `40.599 ms/op` | `+4.45%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.798 ms/op` | `26.614 ms/op` | `+3.16%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.962 ms/op` | `31.864 ms/op` | `+2.91%` | `neutral` |
