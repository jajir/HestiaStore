# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `b20bc5b3bbd7c2ca573d925820e56e7a9db8b1ac`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.104 ms/op` | `92.116 ms/op` | `+12.19%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.839 ms/op` | `63.210 ms/op` | `+11.21%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.014 ms/op` | `74.819 ms/op` | `+22.63%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.302 ms/op` | `41.119 ms/op` | `+7.35%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.052 ms/op` | `27.039 ms/op` | `+3.79%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.421 ms/op` | `32.171 ms/op` | `+2.38%` | `neutral` |
