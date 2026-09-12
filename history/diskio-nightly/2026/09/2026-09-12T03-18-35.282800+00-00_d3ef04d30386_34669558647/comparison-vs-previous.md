# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `79.910 ms/op` | `61.724 ms/op` | `-22.76%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.636 ms/op` | `43.931 ms/op` | `-22.43%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.202 ms/op` | `48.288 ms/op` | `-29.20%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.543 ms/op` | `51.581 ms/op` | `+33.83%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.376 ms/op` | `49.957 ms/op` | `+96.87%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.491 ms/op` | `49.012 ms/op` | `+60.74%` | `better` |
