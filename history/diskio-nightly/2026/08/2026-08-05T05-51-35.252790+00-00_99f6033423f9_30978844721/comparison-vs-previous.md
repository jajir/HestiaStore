# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `65.147 ms/op` | `83.516 ms/op` | `+28.20%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `53.596 ms/op` | `65.147 ms/op` | `+21.55%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `55.330 ms/op` | `61.050 ms/op` | `+10.34%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `82.825 ms/op` | `39.315 ms/op` | `-52.53%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `56.595 ms/op` | `26.158 ms/op` | `-53.78%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `52.360 ms/op` | `31.094 ms/op` | `-40.62%` | `worse` |
