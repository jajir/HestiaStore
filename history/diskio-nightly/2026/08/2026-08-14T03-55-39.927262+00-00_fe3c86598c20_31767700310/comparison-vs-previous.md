# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `102.961 ms/op` | `82.823 ms/op` | `-19.56%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `59.103 ms/op` | `56.237 ms/op` | `-4.85%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `65.470 ms/op` | `61.158 ms/op` | `-6.59%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.599 ms/op` | `38.766 ms/op` | `-4.51%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.614 ms/op` | `25.605 ms/op` | `-3.79%` | `warning` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.864 ms/op` | `30.361 ms/op` | `-4.72%` | `warning` |
