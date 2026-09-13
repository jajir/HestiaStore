# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `61.724 ms/op` | `89.535 ms/op` | `+45.06%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `43.931 ms/op` | `68.859 ms/op` | `+56.74%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `48.288 ms/op` | `66.558 ms/op` | `+37.83%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `51.581 ms/op` | `40.324 ms/op` | `-21.82%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `49.957 ms/op` | `26.547 ms/op` | `-46.86%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `49.012 ms/op` | `32.306 ms/op` | `-34.09%` | `worse` |
