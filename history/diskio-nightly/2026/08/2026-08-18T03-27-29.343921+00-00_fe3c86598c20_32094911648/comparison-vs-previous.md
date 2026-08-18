# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.550 ms/op` | `79.450 ms/op` | `-6.03%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.190 ms/op` | `73.144 ms/op` | `+30.17%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.600 ms/op` | `86.577 ms/op` | `+38.30%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.018 ms/op` | `39.298 ms/op` | `+3.37%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.570 ms/op` | `34.761 ms/op` | `+35.95%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.463 ms/op` | `35.611 ms/op` | `+16.90%` | `better` |
