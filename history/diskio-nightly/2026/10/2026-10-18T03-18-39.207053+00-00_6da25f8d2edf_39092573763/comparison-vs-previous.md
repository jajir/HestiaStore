# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `72.967 ms/op` | `93.973 ms/op` | `+28.79%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `48.346 ms/op` | `61.057 ms/op` | `+26.29%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `57.160 ms/op` | `66.133 ms/op` | `+15.70%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.200 ms/op` | `40.278 ms/op` | `+2.75%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `41.622 ms/op` | `26.122 ms/op` | `-37.24%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `35.204 ms/op` | `31.595 ms/op` | `-10.25%` | `worse` |
