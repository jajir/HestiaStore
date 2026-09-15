# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `93.250 ms/op` | `74.624 ms/op` | `-19.97%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `64.685 ms/op` | `63.033 ms/op` | `-2.55%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.639 ms/op` | `64.694 ms/op` | `-5.75%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.218 ms/op` | `50.977 ms/op` | `+26.75%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.947 ms/op` | `53.406 ms/op` | `+105.83%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.675 ms/op` | `53.314 ms/op` | `+68.32%` | `better` |
