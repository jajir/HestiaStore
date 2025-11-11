# 📊 HestiaStore Benchmark Results

## 🧪 Test Conditions - Read Benchmarks

- Read-focused runs reuse the same controlled JVM, hardware, and JVM flag configuration as the write suite. Each trial prepares a clean directory pointed to by the `dir` system property before preloading the dataset.
- Setup inserts 10 000 000 deterministic key/value pairs (seed `324432L`) so every engine serves identical data. Keys come from `HashDataProvider`, while values remain the constant string `"opice skace po stromech"`.
- Warm-up iterations issue random lookups (80 % hits, 20 % misses) to trigger JIT compilation, cache population, and to ensure index structures have settled before measurements start.
- Measurement iterations continue the same single-threaded read loop, sampling throughput over 20-second windows to capture sustained lookup performance rather than momentary bursts.
- Each benchmark keeps a consistent random sequence per iteration, ensuring engines experience the same access pattern and allowing apples-to-apples comparisons.
- After measurements finish, readers close their resources but the populated directories remain on disk so sizes can be captured by the reporting scripts.
- Tests executed on Mac mini 2024, 16 GB RAM, macOS 15.6.1 (24G90).


## 🏁 Benchmark Results

| Engine       | Score [ops/s]     | ScoreError | Confidence Interval [ops/s] | Occupied space | CPU Usage |
|:--------------|-----------------:|-----------:|-----------------------------:|---------------:|---------:|
| ChronicleMap |       1 682 003 |   292 058 | 1 389 944 .. 1 974 061      | 2.03 GB        | 13%        |
| H2           |         506 792 |    14 165 | 492 627 .. 520 957          | 8 KB           | 11%        |
| HestiaStoreBasic |             669 |        41 | 628 .. 710                  | 507.94 MB      | 11%        |
| HestiaStoreCompress |             749 |        14 | 735 .. 763                  | 283.94 MB      | 11%        |
| LevelDB      |         195 086 |     6 474 | 188 612 .. 201 560          | 363.32 MB      | 10%        |
| MapDB        |           1 264 |        70 | 1 194 .. 1 334              | 1.3 GB         | 4%         |
| RocksDB      |         106 795 |     7 214 | 99 580 .. 114 009           | 324.22 MB      | 11%        |

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

## 📄 Raw JSON Files

### results-read-ChronicleMap-my.json

```json
{
  "totalDirectorySize" : 2177908736,
  "fileCount" : 1,
  "usedMemoryBytes" : 32256120,
  "cpuBefore" : 671175000,
  "cpuAfter" : 1606379000,
  "startTime" : 1648961732540458,
  "endTime" : 1649675242128625,
  "cpuUsage" : 0.13107097865391423
}
```

### results-read-ChronicleMap.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestChronicleMapRead.read",
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
            "-Dengine=ChronicleMapRead"
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
            "score" : 1682002.8268765218,
            "scoreError" : 292058.3572608181,
            "scoreConfidence" : [
                1389944.4696157037,
                1974061.18413734
            ],
            "scorePercentiles" : {
                "0.0" : 197142.9403720747,
                "50.0" : 1806856.0068332641,
                "90.0" : 1890222.1247093529,
                "95.0" : 1897851.6723723745,
                "99.0" : 1901078.5621515624,
                "99.9" : 1901078.5621515624,
                "99.99" : 1901078.5621515624,
                "99.999" : 1901078.5621515624,
                "99.9999" : 1901078.5621515624,
                "100.0" : 1901078.5621515624
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    1820119.3457355907,
                    1831918.3065460657,
                    1806856.0068332641,
                    1828114.833905765,
                    1808762.2744857376,
                    1663180.9878049963,
                    1803128.4114218033,
                    1825953.4242856558,
                    1800829.3752878718,
                    1786706.336570534,
                    1776599.6624524684,
                    1805268.0635949115,
                    1824415.302102971,
                    1703394.6484768006,
                    1536544.8969034227,
                    1668598.8202788664,
                    1810899.5998044407,
                    1730516.169779019,
                    1809892.6676277258,
                    1858056.285219788,
                    671616.1214602407,
                    197142.9403720747,
                    1890322.262887603,
                    1901078.5621515624,
                    1890155.3659238527
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-read-H2-my.json

```json
{
  "totalDirectorySize" : 8192,
  "fileCount" : 1,
  "usedMemoryBytes" : 32616872,
  "cpuBefore" : 635134000,
  "cpuAfter" : 1463415000,
  "startTime" : 1641426355284833,
  "endTime" : 1642149918493208,
  "cpuUsage" : 0.1144725146902063
}
```

### results-read-H2.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestH2Read.read",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=H2Read"
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
            "score" : 506791.9285244222,
            "scoreError" : 14165.03183608771,
            "scoreConfidence" : [
                492626.8966883345,
                520956.9603605099
            ],
            "scorePercentiles" : {
                "0.0" : 471237.70242300315,
                "50.0" : 511293.0890450197,
                "90.0" : 527105.9329954404,
                "95.0" : 534035.4136123367,
                "99.0" : 536639.0343340511,
                "99.9" : 536639.0343340511,
                "99.99" : 536639.0343340511,
                "99.999" : 536639.0343340511,
                "99.9999" : 536639.0343340511,
                "100.0" : 536639.0343340511
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    525337.6218759385,
                    522892.2798470988,
                    501995.8040768004,
                    492388.675720105,
                    513950.5431193879,
                    517392.852333133,
                    500875.32160563004,
                    519771.3696283302,
                    527960.2985950032,
                    511293.0890450197,
                    521623.04335682123,
                    473836.62781834585,
                    520727.6426309597,
                    509364.4466342562,
                    480486.5968320747,
                    471853.824905614,
                    536639.0343340511,
                    526536.3559290653,
                    520603.49371563253,
                    500415.8560454402,
                    485553.5750647521,
                    513741.18706217426,
                    492570.738784536,
                    471237.70242300315,
                    510750.2317273797
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-read-HestiaStoreBasic-my.json

```json
{
  "totalDirectorySize" : 532617108,
  "fileCount" : 10,
  "usedMemoryBytes" : 31454032,
  "cpuBefore" : 845976000,
  "cpuAfter" : 1743708000,
  "startTime" : 1637158023991291,
  "endTime" : 1637952134906833,
  "cpuUsage" : 0.11304869161599121
}
```

### results-read-HestiaStoreBasic.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestHestiaStoreBasicRead.read",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=HestiaStoreBasicRead"
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
            "score" : 668.9071662663633,
            "scoreError" : 41.11066158931372,
            "scoreConfidence" : [
                627.7965046770496,
                710.0178278556771
            ],
            "scorePercentiles" : {
                "0.0" : 552.8465486331146,
                "50.0" : 696.9593337062771,
                "90.0" : 719.9011508373728,
                "95.0" : 723.7053404419349,
                "99.0" : 724.921148360428,
                "99.9" : 724.921148360428,
                "99.99" : 724.921148360428,
                "99.999" : 724.921148360428,
                "99.9999" : 724.921148360428,
                "100.0" : 724.921148360428
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    631.661806861714,
                    552.8465486331146,
                    580.3341018344279,
                    603.5705256292723,
                    663.1782915635563,
                    657.8367832665854,
                    572.9926808774409,
                    609.5723072807286,
                    614.5076053968321,
                    715.6433196022614,
                    612.6223043203357,
                    671.9937661854882,
                    696.9593337062771,
                    713.9905792909677,
                    724.921148360428,
                    714.8873508239324,
                    712.9606361578192,
                    711.6062081606229,
                    711.3822590511212,
                    719.2562811964317,
                    720.8684552987846,
                    714.7264748111861,
                    712.8586920887575,
                    679.3504187306171,
                    702.1512775303793
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-read-HestiaStoreCompress-my.json

```json
{
  "totalDirectorySize" : 297736213,
  "fileCount" : 10,
  "usedMemoryBytes" : 31401000,
  "cpuBefore" : 675596000,
  "cpuAfter" : 1511264000,
  "startTime" : 1647385203175708,
  "endTime" : 1648172315402208,
  "cpuUsage" : 0.1061688501163182
}
```

### results-read-HestiaStoreCompress.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestHestiaStoreCompressRead.read",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=HestiaStoreCompressRead"
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
            "score" : 748.9454404780861,
            "scoreError" : 14.319140539693919,
            "scoreConfidence" : [
                734.6262999383922,
                763.26458101778
            ],
            "scorePercentiles" : {
                "0.0" : 712.2614748019505,
                "50.0" : 746.2646649030198,
                "90.0" : 777.2308012996768,
                "95.0" : 799.7850705472416,
                "99.0" : 808.1926849055343,
                "99.9" : 808.1926849055343,
                "99.99" : 808.1926849055343,
                "99.999" : 808.1926849055343,
                "99.9999" : 808.1926849055343,
                "100.0" : 808.1926849055343
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    755.0830702817915,
                    770.9991349511698,
                    780.1673037112255,
                    808.1926849055343,
                    775.273133025311,
                    748.5834089755748,
                    753.5309075672122,
                    755.7055670343427,
                    751.3107849622255,
                    743.607636318027,
                    712.2614748019505,
                    739.7914230972248,
                    737.4071939539258,
                    743.4076301567964,
                    739.086250200293,
                    746.2646649030198,
                    751.0728302113451,
                    738.7756052965466,
                    752.090197032154,
                    746.3458132218094,
                    737.4341890578321,
                    737.5698580440608,
                    742.2366252285157,
                    725.6202029295067,
                    731.8184220847556
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-read-LevelDB-my.json

```json
{
  "totalDirectorySize" : 380967678,
  "fileCount" : 195,
  "usedMemoryBytes" : 31850312,
  "cpuBefore" : 667283000,
  "cpuAfter" : 1430939000,
  "startTime" : 1649739319381333,
  "endTime" : 1650485889725958,
  "cpuUsage" : 0.10228855264584373
}
```

### results-read-LevelDB.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestLevelDBRead.read",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=LevelDBRead"
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
            "score" : 195085.88678208127,
            "scoreError" : 6474.309123288522,
            "scoreConfidence" : [
                188611.57765879275,
                201560.1959053698
            ],
            "scorePercentiles" : {
                "0.0" : 176190.39792615132,
                "50.0" : 195023.7638549175,
                "90.0" : 210717.30776032349,
                "95.0" : 213571.77150810737,
                "99.0" : 214735.5148025333,
                "99.9" : 214735.5148025333,
                "99.99" : 214735.5148025333,
                "99.999" : 214735.5148025333,
                "99.9999" : 214735.5148025333,
                "100.0" : 214735.5148025333
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    194176.15643904477,
                    198333.2787851288,
                    199667.4447046021,
                    193265.10825709114,
                    195407.03049141855,
                    197508.21639159892,
                    190410.5317169661,
                    210856.3704877802,
                    210624.59927535235,
                    214735.5148025333,
                    205583.4105362529,
                    196543.66525143213,
                    195023.7638549175,
                    197512.81264500812,
                    195650.01538755512,
                    196441.2073694981,
                    182790.2485559269,
                    189268.08888627027,
                    176190.39792615132,
                    190895.46316005214,
                    189254.47328615063,
                    187982.90400167697,
                    189466.4188215005,
                    189178.2307349407,
                    190381.81778318202
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-read-MapDB-my.json

```json
{
  "totalDirectorySize" : 1399848960,
  "fileCount" : 1,
  "usedMemoryBytes" : 31893256,
  "cpuBefore" : 678430000,
  "cpuAfter" : 2660434000,
  "startTime" : 1642150324938458,
  "endTime" : 1647143083053041,
  "cpuUsage" : 0.039697577060881485
}
```

### results-read-MapDB.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestMapDBRead.read",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=MapDBRead"
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
            "score" : 1263.8516542832706,
            "scoreError" : 69.8775734806934,
            "scoreConfidence" : [
                1193.9740808025772,
                1333.729227763964
            ],
            "scorePercentiles" : {
                "0.0" : 947.2129667728096,
                "50.0" : 1284.2584789937207,
                "90.0" : 1339.9479385369061,
                "95.0" : 1385.106205372084,
                "99.0" : 1403.359442120203,
                "99.9" : 1403.359442120203,
                "99.99" : 1403.359442120203,
                "99.999" : 1403.359442120203,
                "99.9999" : 1403.359442120203,
                "100.0" : 1403.359442120203
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    1338.2363511438614,
                    1332.0758755031911,
                    1178.2256174857496,
                    1267.3858818618855,
                    1190.4857155709674,
                    1322.550695069825,
                    1403.359442120203,
                    1342.5153196264732,
                    1299.7816558558081,
                    1330.4724950604327,
                    1319.8257554819095,
                    1324.862598467169,
                    1255.09142594631,
                    1270.262668537638,
                    1081.917561263905,
                    947.2129667728096,
                    1186.4752568233419,
                    1246.3827804523664,
                    1282.7167070422734,
                    1247.307074200635,
                    1309.8857185213003,
                    1293.051064762047,
                    1284.2584789937207,
                    1257.494168092977,
                    1284.458082424962
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-read-RocksDB-my.json

```json
{
  "totalDirectorySize" : 339967184,
  "fileCount" : 12,
  "usedMemoryBytes" : 31535552,
  "cpuBefore" : 640065000,
  "cpuAfter" : 1471724000,
  "startTime" : 1648174332510833,
  "endTime" : 1648907521328250,
  "cpuUsage" : 0.11343039886095196
}
```

### results-read-RocksDB.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestRocksDBRead.read",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=RocksDBRead"
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
            "score" : 106794.51976258818,
            "scoreError" : 7214.086135112554,
            "scoreConfidence" : [
                99580.43362747564,
                114008.60589770073
            ],
            "scorePercentiles" : {
                "0.0" : 92533.94948825406,
                "50.0" : 104900.51756255777,
                "90.0" : 124022.53235463434,
                "95.0" : 124734.48481796458,
                "99.0" : 124851.37397051069,
                "99.9" : 124851.37397051069,
                "99.99" : 124851.37397051069,
                "99.999" : 124851.37397051069,
                "99.9999" : 124851.37397051069,
                "100.0" : 124851.37397051069
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    124851.37397051069,
                    118189.18577054547,
                    122150.12526452326,
                    106745.63460278472,
                    99350.89071646037,
                    95796.16201799439,
                    105270.19129176499,
                    124461.74346202366,
                    123729.72494970812,
                    118383.7650817294,
                    106757.22189820537,
                    106836.36188189387,
                    107846.08853177314,
                    103224.71792558744,
                    102076.18953051057,
                    101304.38590978077,
                    104900.51756255777,
                    108961.56433403351,
                    102582.48822762424,
                    99535.33113220322,
                    100242.10124263776,
                    98587.79501187788,
                    98454.26223849891,
                    97091.2220212208,
                    92533.94948825406
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```
