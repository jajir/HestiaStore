# Development

Here are some development related topiscs.

## How to run JMH benchmarks

Follow this steps:
* Compile whole project and create pacakge containing all benchmarks data
* Go to `jmh-benchmarks`
* Execute it, with temp directory in `target` directory

```bash
mvn clean install
cd jmh-benchmarks
java -Ddir=./target/ -jar target/jmh-benchmarks.jar
```

Specific JMH benchmark class could be run:

```bash
java -Ddir=./target/ -jar target/jmh-benchmarks.jar SegmentSearchBenchmark
```

result could look like:

```
Benchmark                                             Mode  Cnt    Score      Error  Units
SequentialFileReadingBenchmark.test_with_buffer_01KB  avgt    4   70.747 ±   42.480  ms/op
SequentialFileReadingBenchmark.test_with_buffer_02KB  avgt    4   60.009 ±   52.899  ms/op
SequentialFileReadingBenchmark.test_with_buffer_04KB  avgt    4   51.254 ±   30.112  ms/op
SequentialFileReadingBenchmark.test_with_buffer_08KB  avgt    4   48.600 ±   28.892  ms/op
SequentialFileReadingBenchmark.test_with_buffer_16KB  avgt    4   48.471 ±   25.665  ms/op
SequentialFileReadingBenchmark.test_with_buffer_32KB  avgt    4   45.256 ±   24.986  ms/op
SequentialFileReadingBenchmark.test_with_buffer_64KB  avgt    4   45.204 ±   24.867  ms/op
SequentialFileWritingBenchmark.test_with_buffer_01KB  avgt    4  238.075 ±   75.311  ms/op
SequentialFileWritingBenchmark.test_with_buffer_02KB  avgt    4  271.272 ±   64.747  ms/op
SequentialFileWritingBenchmark.test_with_buffer_04KB  avgt    4  276.001 ±   45.815  ms/op
SequentialFileWritingBenchmark.test_with_buffer_08KB  avgt    4  352.189 ± 1140.814  ms/op
SequentialFileWritingBenchmark.test_with_buffer_16KB  avgt    4  258.806 ±   44.693  ms/op
SequentialFileWritingBenchmark.test_with_buffer_32KB  avgt    4  276.246 ±  135.019  ms/op
SequentialFileWritingBenchmark.test_with_buffer_64KB  avgt    4  275.944 ±  128.835  ms/op
```

When some JMH benchmark class is changed command `mvn package` have to be run.

### Possible problems

Generally JMH is quite fragile. Small changes broke JMH benchmark execution. Usually helps rebuild project and start again as described above.

## Load test

Runnign JVM should be inspected with some profiller. For profilling is usefull to hae long running task to watch it. Go to project `load-test`. Following command show all optional parameters:

```bash
java -jar target/load-test.jar com.coroptis.index.loadtest.Main --help
```

Theer are two main supported operations. First is data generating. It's could be usefull to place in java profilling agent. It could look like:

```bash
java \
    -agentpath:/Applications/YourKit-Java-Profiler-2023.9.app/Contents/Resources/bin/mac/libyjpagent.dylib=exceptions=disable,delay=10000,listen=all \
    -jar target/load-test.jar com.coroptis.index.loadtest.Main \
    --write \
    --directory /Volumes/LaCie/test/  \
    --count 5_000_000_000 \
    --max-number-of-keys-in-cache 5_000_000 \
    --max-number-of-keys-in-segment 10_000_000 \
    --max-number-of-keys-in-segment-cache 500_000 \
    --max-number-of-keys-in-segment-cache-during-flushing 2_000_000 \
    --max-number-of-keys-in-segment-index-page 1_000 \
    --bloom-filter-index-size-in-bytes 10_000_000 \
    --bloom-filter-number-of-hash-functions 2
```



## Development

Mockito requires reflective access to non-public parts in a Java module. It could be manually open by passing following parameter as jvm parameter:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
```



## How to get segment disk size

On apple try:

```
diskutil  info /Volumes/LaCie
```
