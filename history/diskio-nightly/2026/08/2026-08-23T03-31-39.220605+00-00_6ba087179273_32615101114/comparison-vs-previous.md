# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.759 ms/op` | `83.012 ms/op` | `+0.31%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `62.828 ms/op` | `53.610 ms/op` | `-14.67%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.883 ms/op` | `62.663 ms/op` | `-0.35%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.023 ms/op` | `38.963 ms/op` | `-0.15%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.603 ms/op` | `25.878 ms/op` | `+1.08%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.817 ms/op` | `31.059 ms/op` | `+0.78%` | `neutral` |
