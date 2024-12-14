#How to run JMH benchmarks

Follow this steps:
* Go to `jmh-benchmarks`
* Create pacakge containing all benchmarks data
* Execute it

```bash
cd jmh-benchmarks
mvn package
java -jar target/benchmarks.jar
```
