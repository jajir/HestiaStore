# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `72.021 ms/op` | `88.878 ms/op` | `+23.41%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `49.418 ms/op` | `56.500 ms/op` | `+14.33%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `54.972 ms/op` | `62.812 ms/op` | `+14.26%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `57.405 ms/op` | `37.994 ms/op` | `-33.81%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `50.760 ms/op` | `25.539 ms/op` | `-49.69%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `53.480 ms/op` | `30.526 ms/op` | `-42.92%` | `worse` |
