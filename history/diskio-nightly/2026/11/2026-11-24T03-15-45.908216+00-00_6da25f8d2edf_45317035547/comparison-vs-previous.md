# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `63.625 ms/op` | `89.114 ms/op` | `+40.06%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `47.140 ms/op` | `58.951 ms/op` | `+25.05%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `49.133 ms/op` | `63.458 ms/op` | `+29.16%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `42.195 ms/op` | `41.264 ms/op` | `-2.21%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `31.973 ms/op` | `29.210 ms/op` | `-8.64%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.705 ms/op` | `32.168 ms/op` | `+4.77%` | `better` |
