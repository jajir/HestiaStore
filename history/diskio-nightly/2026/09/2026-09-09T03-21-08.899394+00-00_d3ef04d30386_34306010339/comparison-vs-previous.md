# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.262 ms/op` | `74.326 ms/op` | `-11.79%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.624 ms/op` | `50.087 ms/op` | `-11.54%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.541 ms/op` | `54.074 ms/op` | `-12.13%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.281 ms/op` | `72.090 ms/op` | `+88.32%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.488 ms/op` | `58.079 ms/op` | `+127.87%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.576 ms/op` | `55.362 ms/op` | `+81.06%` | `better` |
