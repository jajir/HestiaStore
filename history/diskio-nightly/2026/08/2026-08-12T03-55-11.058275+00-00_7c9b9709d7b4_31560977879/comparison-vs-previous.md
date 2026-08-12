# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.851 ms/op` | `83.247 ms/op` | `+0.48%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `67.233 ms/op` | `57.457 ms/op` | `-14.54%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `72.033 ms/op` | `63.009 ms/op` | `-12.53%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `62.799 ms/op` | `38.871 ms/op` | `-38.10%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `60.143 ms/op` | `25.798 ms/op` | `-57.11%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `58.243 ms/op` | `30.962 ms/op` | `-46.84%` | `worse` |
