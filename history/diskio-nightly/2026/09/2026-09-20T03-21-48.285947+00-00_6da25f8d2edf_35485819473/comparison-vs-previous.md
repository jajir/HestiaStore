# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `73.837 ms/op` | `83.496 ms/op` | `+13.08%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `54.667 ms/op` | `56.467 ms/op` | `+3.29%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `55.888 ms/op` | `63.398 ms/op` | `+13.44%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `54.104 ms/op` | `38.311 ms/op` | `-29.19%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `55.053 ms/op` | `25.918 ms/op` | `-52.92%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `50.604 ms/op` | `30.684 ms/op` | `-39.36%` | `worse` |
