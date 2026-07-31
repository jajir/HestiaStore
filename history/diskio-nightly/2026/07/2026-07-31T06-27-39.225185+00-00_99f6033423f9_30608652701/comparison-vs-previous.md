# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `4c62ce188ffb07fdedd6c5d5d57d0453a33563a3`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `85.060 ms/op` | `84.943 ms/op` | `-0.14%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `54.688 ms/op` | `55.759 ms/op` | `+1.96%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.836 ms/op` | `61.233 ms/op` | `-0.98%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.475 ms/op` | `38.300 ms/op` | `-0.45%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.979 ms/op` | `25.523 ms/op` | `-1.76%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.861 ms/op` | `30.237 ms/op` | `-2.02%` | `neutral` |
