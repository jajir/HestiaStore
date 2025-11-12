# HestiaStore Benchmark Results

## Test Conditions - Sequential Read Benchmarks

- Each sequential scenario uses the same JVM flags, hardware, and scratch directory handling as the write/read suites. The `dir` property is cleaned before every run to guarantee a fresh start.
- Setup writes 10 000 000 deterministic key/value pairs (seed `324432L`) into the engine. Keys are generated via `HashDataProvider` so that the exact ordering is reproducible across runs.
- After preloading, the benchmark resets its sequential cursor. Warm-up iterations walk the keyspace from the first key to the last key so caches and OS I/O buffers reflect streaming access.
- Measurement iterations continue sequential scans, looping back to the first key whenever the end is reached. This focuses on sustained read throughput when data is consumed in order.
- The read workload remains single-threaded; each invocation issues exactly one lookup to keep measurements comparable with the other suites.
- Directories remain on disk after the run so disk usage and auxiliary metrics can be collected by reporting scripts.
- Tests for HestiaStoreStream use dedicated stream API. Without using Stream API is performance visible in line HestiaStoreBasic.
- Tests executed on Mac mini 2024, 16 GB RAM, macOS 15.6.1 (24G90).


## Benchmark Results

| Engine       | Score [ops/s]     | ScoreError | Confidence Interval [ops/s] | Occupied space | CPU Usage |
|:--------------|-----------------:|-----------:|-----------------------------:|---------------:|---------:|
| ChronicleMap |       1 707 702 |    82 988 | 1 624 713 .. 1 790 690      | 2.03 GB        | 13%        |
| H2           |         364 687 |    43 577 | 321 110 .. 408 264          | 8 KB           | 14%        |
| HestiaStoreBasic |             592 |        68 | 524 .. 660                  | 507.94 MB      | 15%        |
| HestiaStoreStream |       4 792 777 |   144 132 | 4 648 646 .. 4 936 909      | 283.94 MB      | 12%        |
| LevelDB      |         190 698 |     6 694 | 184 004 .. 197 391          | 363.32 MB      | 10%        |
| MapDB        |           1 528 |       228 | 1 300 .. 1 756              | 1.3 GB         | 5%         |
| RocksDB      |         109 551 |    10 513 | 99 038 .. 120 064           | 324.23 MB      | 10%        |

meaning of columns:

- Engine: name of the benchmarked engine (as derived from the JSON filename)
- Score [ops/s]: number of operations per second (higher is better)
- ScoreError: error margin of the score (lower is better). It's computed as `z * (stdev / sqrt(n)) where`
  - `z` is the z-score for the desired confidence level (1.96 for 95%)
  - `stdev` is the standard deviation of the measurements
  - `n` is the number of measurements
- Confidence Interval: 95% confidence interval of the score (lower and upper bound). This means that the true mean is likely between this interval of ops/sec. Negative values are possible if the error margin is larger than the score itself.
- Occupied space : amount of disk space occupied by the engine's data structures (lower is better). It is measured after flushing last data to disk.
- CPU Usage: average CPU usage during the benchmark (lower is better). Please note, that it includes all system processes, not only the benchmarked engine.

## Raw JSON Files

### results-sequential-ChronicleMap-my.json

```json
{
  "totalDirectorySize" : 2177908736,
  "fileCount" : 1,
  "usedMemoryBytes" : 32422400,
  "cpuBefore" : 801274000,
  "cpuAfter" : 1755750000,
  "startTime" : 1703675458276666,
  "endTime" : 1704389298092541,
  "cpuUsage" : 0.13371010957549861
}
```

### results-sequential-ChronicleMap.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestChronicleMapSequential.readSequential",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.model=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.processing=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
            "-Dengine=ChronicleMapSequential"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 1707701.5393647857,
            "scoreError" : 82988.49124709374,
            "scoreConfidence" : [
                1624713.048117692,
                1790690.0306118794
            ],
            "scorePercentiles" : {
                "0.0" : 1493645.6848657392,
                "50.0" : 1697235.2876620279,
                "90.0" : 1866914.805183605,
                "95.0" : 1949117.6912285273,
                "99.0" : 1982288.7646156966,
                "99.9" : 1982288.7646156966,
                "99.99" : 1982288.7646156966,
                "99.999" : 1982288.7646156966,
                "99.9999" : 1982288.7646156966,
                "100.0" : 1982288.7646156966
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    1696219.9125454635,
                    1729623.9236061238,
                    1610710.1801204153,
                    1769414.6429731457,
                    1667978.9665652176,
                    1697235.2876620279,
                    1615269.9628077352,
                    1645186.2274296894,
                    1609251.5083761679,
                    1532777.2358683166,
                    1656807.1692482952,
                    1714353.3714364078,
                    1632212.814084742,
                    1602141.1933655317,
                    1493645.6848657392,
                    1728442.407270448,
                    1663535.869794149,
                    1871718.5199917993,
                    1750647.1939896245,
                    1782203.5382766959,
                    1748345.4161347644,
                    1863712.3286448088,
                    1804973.0103171149,
                    1823843.3541295172,
                    1982288.7646156966
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-sequential-H2-my.json

```json
{
  "totalDirectorySize" : 8192,
  "fileCount" : 1,
  "usedMemoryBytes" : 31557248,
  "cpuBefore" : 894304000,
  "cpuAfter" : 1925876000,
  "startTime" : 1688895819370416,
  "endTime" : 1689623148230958,
  "cpuUsage" : 0.14183020308465147
}
```

### results-sequential-H2.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestH2Sequential.readSequential",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=H2Sequential"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 364686.8795874021,
            "scoreError" : 43576.97512676437,
            "scoreConfidence" : [
                321109.9044606377,
                408263.8547141665
            ],
            "scorePercentiles" : {
                "0.0" : 241753.277608701,
                "50.0" : 370385.7523692228,
                "90.0" : 446029.59125580837,
                "95.0" : 448741.1111258906,
                "99.0" : 449399.69289311557,
                "99.9" : 449399.69289311557,
                "99.99" : 449399.69289311557,
                "99.999" : 449399.69289311557,
                "99.9999" : 449399.69289311557,
                "100.0" : 449399.69289311557
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    370385.7523692228,
                    408171.4525145759,
                    447204.42033569893,
                    364287.3952151101,
                    449399.69289311557,
                    398253.93659871473,
                    442632.17906150565,
                    426731.13119361637,
                    402697.67651350296,
                    376196.5657093395,
                    362036.4249909497,
                    403956.13641543523,
                    297643.9869127228,
                    342528.82342547516,
                    309758.55097084236,
                    270084.8363388006,
                    328622.4460196818,
                    241753.277608701,
                    310612.8076248173,
                    347948.04101550026,
                    326277.84435612155,
                    385128.5393856402,
                    286516.83631126373,
                    373096.8640354852,
                    445246.37186921464
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-sequential-HestiaStoreBasic-my.json

```json
{
  "totalDirectorySize" : 532617107,
  "fileCount" : 10,
  "usedMemoryBytes" : 31775872,
  "cpuBefore" : 995124000,
  "cpuAfter" : 2351232000,
  "startTime" : 1694852769687083,
  "endTime" : 1695740649510708,
  "cpuUsage" : 0.15273553513845342
}
```

### results-sequential-HestiaStoreBasic.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestHestiaStoreBasicSequential.readSequential",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=HestiaStoreBasicSequential"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 591.9892599976968,
            "scoreError" : 68.32781871990737,
            "scoreConfidence" : [
                523.6614412777894,
                660.3170787176041
            ],
            "scorePercentiles" : {
                "0.0" : 217.07036397533005,
                "50.0" : 614.7650530363439,
                "90.0" : 665.1517330489332,
                "95.0" : 670.7843310676988,
                "99.0" : 672.2055421288844,
                "99.9" : 672.2055421288844,
                "99.99" : 672.2055421288844,
                "99.999" : 672.2055421288844,
                "99.9999" : 672.2055421288844,
                "100.0" : 672.2055421288844
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    651.9451860971615,
                    530.8595105637474,
                    634.8872162131643,
                    558.0429463121978,
                    553.4575737726775,
                    554.2370536905249,
                    488.09095369510084,
                    614.7650530363439,
                    552.3371818406346,
                    217.07036397533005,
                    610.9353019515233,
                    591.8184880269033,
                    650.6265428969912,
                    628.1819004580707,
                    633.6255856581995,
                    616.504191610874,
                    587.2887115484536,
                    663.6074404649339,
                    639.37017713768,
                    642.0884344099126,
                    672.2055421288844,
                    667.4681719249323,
                    648.9143712953814,
                    607.0026495079342,
                    584.4009517248626
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-sequential-HestiaStoreStream-my.json

```json
{
  "totalDirectorySize" : 297736210,
  "fileCount" : 10,
  "usedMemoryBytes" : 31879856,
  "cpuBefore" : 705656000,
  "cpuAfter" : 1654368000,
  "startTime" : 1738846165288208,
  "endTime" : 1739625462973208,
  "cpuUsage" : 0.12173935817607363
}
```

### results-sequential-HestiaStoreStream.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestHestiaStoreCompressSequential2.readSequentialStream",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=HestiaStoreCompressSequential2"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 4792777.446329955,
            "scoreError" : 144131.89712030266,
            "scoreConfidence" : [
                4648645.5492096525,
                4936909.343450258
            ],
            "scorePercentiles" : {
                "0.0" : 4253292.20242851,
                "50.0" : 4793888.0753746815,
                "90.0" : 4990279.774572669,
                "95.0" : 5013138.2306711,
                "99.0" : 5014745.693444231,
                "99.9" : 5014745.693444231,
                "99.99" : 5014745.693444231,
                "99.999" : 5014745.693444231,
                "99.9999" : 5014745.693444231,
                "100.0" : 5014745.693444231
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    4940897.010906559,
                    4811267.514070776,
                    4793888.0753746815,
                    4253292.20242851,
                    4445228.221274129,
                    4374010.689731631,
                    4708902.625330204,
                    4950020.989303642,
                    4977541.301487473,
                    4870900.688786804,
                    5009387.484200462,
                    4930047.971332142,
                    4927043.182530843,
                    4728505.515544281,
                    5014745.693444231,
                    4908131.309611777,
                    4971694.40708881,
                    4747109.83595018,
                    4782049.903774503,
                    4891229.719922479,
                    4793055.605468624,
                    4765577.03327685,
                    4763952.21497315,
                    4771201.630097632,
                    4689755.332338488
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-sequential-LevelDB-my.json

```json
{
  "totalDirectorySize" : 380967678,
  "fileCount" : 195,
  "usedMemoryBytes" : 31418048,
  "cpuBefore" : 654382000,
  "cpuAfter" : 1410956000,
  "startTime" : 1697277383262041,
  "endTime" : 1698020913951000,
  "cpuUsage" : 0.10175423976907554
}
```

### results-sequential-LevelDB.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestLevelDBSequential.readSequential",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=LevelDBSequential"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 190697.53218919376,
            "scoreError" : 6693.958699915936,
            "scoreConfidence" : [
                184003.57348927783,
                197391.4908891097
            ],
            "scorePercentiles" : {
                "0.0" : 159796.8509345367,
                "50.0" : 191873.73089029174,
                "90.0" : 199452.94601770226,
                "95.0" : 203332.36724773305,
                "99.0" : 204914.9378197743,
                "99.9" : 204914.9378197743,
                "99.99" : 204914.9378197743,
                "99.999" : 204914.9378197743,
                "99.9999" : 204914.9378197743,
                "100.0" : 204914.9378197743
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    196891.7955830414,
                    204914.9378197743,
                    197630.38717919897,
                    199328.44164307916,
                    199639.70257963688,
                    159796.8509345367,
                    179592.90318773626,
                    177867.3502066716,
                    187103.89915090462,
                    195989.84190748248,
                    198468.47083921975,
                    194885.05032325233,
                    195842.15284018134,
                    191111.97083935005,
                    186822.94025591918,
                    189569.6139971288,
                    191917.1063035356,
                    187017.16397061732,
                    185214.37271751274,
                    188873.28992909336,
                    192111.93632112874,
                    189995.6940470095,
                    194754.4095676854,
                    190224.2916958571,
                    191873.73089029174
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-sequential-MapDB-my.json

```json
{
  "totalDirectorySize" : 1399848960,
  "fileCount" : 1,
  "usedMemoryBytes" : 31462464,
  "cpuBefore" : 769731000,
  "cpuAfter" : 3332809000,
  "startTime" : 1689623718364083,
  "endTime" : 1694851860176208,
  "cpuUsage" : 0.04902464569831984
}
```

### results-sequential-MapDB.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestMapDBSequential.readSequential",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=MapDBSequential"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 1527.9789090701086,
            "scoreError" : 228.215230350306,
            "scoreConfidence" : [
                1299.7636787198026,
                1756.1941394204146
            ],
            "scorePercentiles" : {
                "0.0" : 1090.761945951313,
                "50.0" : 1411.1236203864682,
                "90.0" : 1971.777273993194,
                "95.0" : 1995.0192649747119,
                "99.0" : 2001.3620984483193,
                "99.9" : 2001.3620984483193,
                "99.99" : 2001.3620984483193,
                "99.999" : 2001.3620984483193,
                "99.9999" : 2001.3620984483193,
                "100.0" : 2001.3620984483193
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    1980.2193202029612,
                    1799.0554072176392,
                    1746.3774023452725,
                    1966.0394570221047,
                    2001.3620984483193,
                    1955.017761157936,
                    1966.1492431866825,
                    1753.9545467605956,
                    1724.4810704849083,
                    1777.462895610476,
                    1445.6085835239599,
                    1411.1236203864682,
                    1474.871640194682,
                    1156.897990609939,
                    1127.2417106851822,
                    1322.745669290138,
                    1268.1836611199321,
                    1090.761945951313,
                    1175.9389600674933,
                    1375.5142570523617,
                    1317.1282852871068,
                    1395.8129273532918,
                    1292.3779771455322,
                    1326.9411137886998,
                    1348.205181859728
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-sequential-RocksDB-my.json

```json
{
  "totalDirectorySize" : 339977964,
  "fileCount" : 12,
  "usedMemoryBytes" : 31820496,
  "cpuBefore" : 764980000,
  "cpuAfter" : 1492221000,
  "startTime" : 1696542435579291,
  "endTime" : 1697276855630958,
  "cpuUsage" : 0.09902248697449029
}
```

### results-sequential-RocksDB.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestRocksDBSequential.readSequential",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=RocksDBSequential"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 109550.92199554075,
            "scoreError" : 10512.710963722118,
            "scoreConfidence" : [
                99038.21103181863,
                120063.63295926286
            ],
            "scorePercentiles" : {
                "0.0" : 84995.28542762823,
                "50.0" : 105854.72446603306,
                "90.0" : 134000.91427096617,
                "95.0" : 136675.9566224093,
                "99.0" : 136716.71773725914,
                "99.9" : 136716.71773725914,
                "99.99" : 136716.71773725914,
                "99.999" : 136716.71773725914,
                "99.9999" : 136716.71773725914,
                "100.0" : 136716.71773725914
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    96192.28900175112,
                    98677.08420993124,
                    102449.99957338264,
                    105634.93036960272,
                    102474.29135688648,
                    113627.64127716908,
                    114310.33693810109,
                    114442.7830600832,
                    119967.77141802286,
                    112527.36307250132,
                    114575.93837877734,
                    117137.70558750023,
                    105854.72446603306,
                    84995.28542762823,
                    93430.56299680991,
                    93438.21018397433,
                    99910.11169119111,
                    117520.68636346157,
                    99931.16898666933,
                    94249.24893787173,
                    102407.58589720975,
                    129438.80672028179,
                    132280.95888199267,
                    136580.8473544264,
                    136716.71773725914
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```
