# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `93.283 ms/op` | `82.851 ms/op` | `-11.18%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `61.666 ms/op` | `67.233 ms/op` | `+9.03%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.001 ms/op` | `72.033 ms/op` | `+5.93%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.872 ms/op` | `62.799 ms/op` | `+57.50%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.401 ms/op` | `60.143 ms/op` | `+127.80%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.716 ms/op` | `58.243 ms/op` | `+83.64%` | `better` |
