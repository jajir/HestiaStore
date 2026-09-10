# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `74.326 ms/op` | `83.247 ms/op` | `+12.00%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `50.087 ms/op` | `56.327 ms/op` | `+12.46%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `54.074 ms/op` | `61.286 ms/op` | `+13.34%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `72.090 ms/op` | `39.060 ms/op` | `-45.82%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `58.079 ms/op` | `25.916 ms/op` | `-55.38%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `55.362 ms/op` | `31.646 ms/op` | `-42.84%` | `worse` |
