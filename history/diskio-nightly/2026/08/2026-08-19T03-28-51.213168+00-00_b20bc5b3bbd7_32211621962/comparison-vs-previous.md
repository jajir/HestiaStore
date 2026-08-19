# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `b20bc5b3bbd7c2ca573d925820e56e7a9db8b1ac`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `79.450 ms/op` | `82.104 ms/op` | `+3.34%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `73.144 ms/op` | `56.839 ms/op` | `-22.29%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `86.577 ms/op` | `61.014 ms/op` | `-29.53%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.298 ms/op` | `38.302 ms/op` | `-2.53%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `34.761 ms/op` | `26.052 ms/op` | `-25.05%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `35.611 ms/op` | `31.421 ms/op` | `-11.77%` | `worse` |
