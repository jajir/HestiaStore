# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.012 ms/op` | `83.783 ms/op` | `+0.93%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `53.610 ms/op` | `56.487 ms/op` | `+5.37%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.663 ms/op` | `63.050 ms/op` | `+0.62%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.963 ms/op` | `38.508 ms/op` | `-1.17%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.878 ms/op` | `25.560 ms/op` | `-1.23%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.059 ms/op` | `30.374 ms/op` | `-2.20%` | `neutral` |
