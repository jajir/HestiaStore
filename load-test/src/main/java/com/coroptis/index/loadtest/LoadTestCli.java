package com.coroptis.index.loadtest;

import java.io.File;
import java.util.Objects;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FsDirectory;
import com.coroptis.index.sst.Index;

public class LoadTestCli {

        private final static TypeDescriptor<String> TYPE_DESCRIPTOR_STRING = new TypeDescriptorString();
        private final static TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();

        private final static Option OPTION_HELP = Option.builder()//
                        .longOpt("help")//
                        .hasArg(false)//
                        .desc("display help")//
                        .build();

        // Write and related parameters
        private final static Option OPTION_WRITE = Option.builder()//
                        .longOpt("write")//
                        .hasArg(false)//
                        .desc("write new random data into index.")//
                        .build();

        private final static Option OPTION_COUNT = Option.builder("c")//
                        .longOpt("count")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("How many key will be written").build();

        // Search and related parameters
        private final static Option OPTION_SEARCH = Option.builder()//
                        .longOpt("search")//
                        .hasArg(false)//
                        .desc("search random data from index.")//
                        .build();

        private final static Option OPTION_MAX_KEY = Option.builder()//
                        .longOpt("max-key")//
                        .hasArg(false)//
                        .required(false)//
                        .desc("Max key value to search").build();

        // commmon index parameters

        private final static Option OPTION_DIRECTORY = Option.builder()//
                        .longOpt("directory")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("directory where index lies, when user selects count or search task then this parameter is mandatory")//
                        .build();

        private final static Option OPTION_CUSTOM_CONF = Option.builder()//
                        .longOpt("custom-conf")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("Build with custom configuration, default is true")
                        .build();

        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT = Option
                        .builder()//
                        .longOpt("max-number-of-keys-in-segment")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("Max number of keys in segment").build();
        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = Option
                        .builder()//
                        .longOpt("max-number-of-keys-in-segment-cache")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("Max number of keys in segment cache").build();
        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = Option
                        .builder()//
                        .longOpt("max-number-of-keys-in-segment-cache-during-flushing")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("Max number of keys in segment cache during flushing")
                        .build();
        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE = Option
                        .builder()//
                        .longOpt("max-number-of-keys-in-segment-index-page")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("Max number of keys in segment index page").build();
        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_CACHE = Option
                        .builder()//
                        .longOpt("max-number-of-keys-in-cache")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("Max number of keys in cache").build();
        private final static Option OPTION_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = Option
                        .builder()//
                        .longOpt("bloom-filter-index-size-in-bytes")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("Bloom filter index size in bytes").build();
        private final static Option OPTION_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = Option
                        .builder()//
                        .longOpt("bloom-filter-number-of-hash-functions")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("Bloom filter number of hash functions").build();

        LoadTestCli(final String[] args) throws ParseException {
                final Options options = new Options();
                options.addOption(OPTION_HELP);
                options.addOption(OPTION_DIRECTORY);
                options.addOption(OPTION_COUNT);
                options.addOption(OPTION_WRITE);
                options.addOption(OPTION_SEARCH);
                options.addOption(OPTION_MAX_KEY);
                options.addOption(OPTION_CUSTOM_CONF);
                options.addOption(OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT);
                options.addOption(OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
                options.addOption(
                                OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING);
                options.addOption(
                                OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE);
                options.addOption(OPTION_MAX_NUMBER_OF_KEYS_IN_CACHE);
                options.addOption(OPTION_BLOOM_FILTER_INDEX_SIZE_IN_BYTES);
                options.addOption(OPTION_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS);

                final CommandLineParser parser = new DefaultParser();
                final CommandLine cmd = parser.parse(options, args);
                if (cmd.hasOption(OPTION_HELP)) {
                        final HelpFormatter formatter = new HelpFormatter();
                        formatter.setWidth(120);
                        formatter.printHelp(
                                        "java -jar target/load-test.jar com.coroptis.index.loadtest.Main",
                                        options);
                } else if (cmd.hasOption(OPTION_WRITE)) {
                        handleWriteOption(cmd);
                } else if (cmd.hasOption(OPTION_SEARCH)) {
                        handleSearchOption(cmd);
                } else {
                        throw new IllegalArgumentException(
                                        "Unknown command. There must be --help, "
                                                        + "--search or --write");
                }
        }

        private void handleWriteOption(final CommandLine cmd) {
                final String directory = extractDirectoryOption(cmd);
                final long count = extractCountOption(cmd);
                //FIXME it's not used
                // final long customConf = extractCustomConfOption(cmd);
                final long maxNumberOfKeysInSegment = extractMaxNumberOfKeysInSegmentOption(
                                cmd);
                final long maxNumberOfKeysInSegmentCache = extractMaxNumberOfKeysInSegmentCacheOption(
                                cmd);
                final long maxNumberOfKeysInSegmentCacheDuringFlushing = extractMaxNumberOfKeysInSegmentCacheDuringFlushingOption(
                                cmd);
                final long maxNumberOfKeysInSegmentIndexPage = extractMaxNumberOfKeysInSegmentIndexPageOption(
                                cmd);
                final long maxNumberOfKeysInCache = extractMaxNumberOfKeysInCacheOption(
                                cmd);
                final long bloomFilterIndexSizeInBytes = extractBloomFilterIndexSizeInBytesOption(
                                cmd);
                final long bloomFilterNumberOfHashFunctions = extractBloomFilterNumberOfHashFunctionsOption(
                                cmd);

                System.out.println("directory: " + directory);
                System.out.println("count: " + count);
                System.out.println("customConf: " + customConf);
                System.out.println("maxNumberOfKeysInSegment: "
                                + maxNumberOfKeysInSegment);
                System.out.println("maxNumberOfKeysInSegmentCache: "
                                + maxNumberOfKeysInSegmentCache);
                System.out.println(
                                "maxNumberOfKeysInSegmentCacheDuringFlushing: "
                                                + maxNumberOfKeysInSegmentCacheDuringFlushing);
                System.out.println("maxNumberOfKeysInSegmentIndexPage: "
                                + maxNumberOfKeysInSegmentIndexPage);
                System.out.println("maxNumberOfKeysInCache: "
                                + maxNumberOfKeysInCache);
                System.out.println("bloomFilterIndexSizeInBytes: "
                                + bloomFilterIndexSizeInBytes);
                System.out.println("bloomFilterNumberOfHashFunctions: "
                                + bloomFilterNumberOfHashFunctions);

                final Directory dir = new FsDirectory(new File(directory));

                final Index<String, Long> index = Index.<String, Long>builder()//
                                .withDirectory(dir)//
                                .withKeyClass(String.class)//
                                .withValueClass(Long.class)//
                                .withKeyTypeDescriptor(TYPE_DESCRIPTOR_STRING) //
                                .withValueTypeDescriptor(TYPE_DESCRIPTOR_LONG) //
                                .withCustomConf()//
                                .withMaxNumberOfKeysInSegment(
                                                (int) maxNumberOfKeysInSegment) //
                                .withMaxNumberOfKeysInSegmentCache(
                                                maxNumberOfKeysInSegmentCache) //
                                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(
                                                (int) maxNumberOfKeysInSegmentCacheDuringFlushing) //
                                .withMaxNumberOfKeysInSegmentIndexPage(
                                                (int) maxNumberOfKeysInSegmentIndexPage) //
                                .withMaxNumberOfKeysInCache(
                                                (int) maxNumberOfKeysInCache) //
                                .withBloomFilterIndexSizeInBytes(
                                                (int) bloomFilterIndexSizeInBytes) //
                                .withBloomFilterNumberOfHashFunctions(
                                                (int) bloomFilterNumberOfHashFunctions) //
                                .withUseFullLog(false) //
                                .build();
                final WriteData writeData = new WriteData(index);
                writeData.write(count);
        }

        private void handleSearchOption(final CommandLine cmd) {
                // Empty method for handling search option
        }

        private long extractCountOption(final CommandLine cmd) {
                if (cmd.hasOption(OPTION_COUNT)) {
                        return parseLong(cmd
                                        .getOptionValue(OPTION_COUNT));
                } else {
                        throw new IllegalArgumentException(
                                        "When you select write task then you must specify count of keys");
                }
        }

        private long extractCustomConfOption(final CommandLine cmd) {
                if (cmd.hasOption(OPTION_CUSTOM_CONF)) {
                        return parseLong(cmd.getOptionValue(
                                        OPTION_CUSTOM_CONF));
                } else {
                        throw new IllegalArgumentException(
                                        "When you select this task then you must specify custom configuration");
                }
        }

        private long extractMaxNumberOfKeysInSegmentOption(
                        final CommandLine cmd) {
                if (cmd.hasOption("max-number-of-keys-in-segment")) {
                        return parseLong(cmd.getOptionValue("max-number-of-keys-in-segment"));
                } else {
                        throw new IllegalArgumentException(
                                        "When you select this task then you must specify max number of keys in segment");
                }
        }

        private long extractMaxNumberOfKeysInSegmentCacheOption(
                        final CommandLine cmd) {
                if (cmd.hasOption("max-number-of-keys-in-segment-cache")) {
                        return parseLong(cmd.getOptionValue("max-number-of-keys-in-segment-cache"));
                } else {
                        throw new IllegalArgumentException(
                                        "When you select this task then you must specify max number of keys in segment cache");
                }
        }

        private long extractMaxNumberOfKeysInSegmentCacheDuringFlushingOption(
                        final CommandLine cmd) {
                if (cmd.hasOption("max-number-of-keys-in-segment-cache-during-flushing")) {
                        return parseLong(cmd.getOptionValue("max-number-of-keys-in-segment-cache-during-flushing"));
                } else {
                        throw new IllegalArgumentException(
                                        "When you select this task then you must specify max number of keys in segment cache during flushing");
                }
        }

        private long extractMaxNumberOfKeysInSegmentIndexPageOption(
                        final CommandLine cmd) {
                if (cmd.hasOption("max-number-of-keys-in-segment-index-page")) {
                        return parseLong(cmd.getOptionValue("max-number-of-keys-in-segment-index-page"));
                } else {
                        throw new IllegalArgumentException(
                                        "When you select this task then you must specify max number of keys in segment index page");
                }
        }

        private long extractMaxNumberOfKeysInCacheOption(
                        final CommandLine cmd) {
                if (cmd.hasOption("max-number-of-keys-in-cache")) {
                        return parseLong(cmd.getOptionValue("max-number-of-keys-in-cache"));
                } else {
                        throw new IllegalArgumentException(
                                        "When you select this task then you must specify max number of keys in cache");
                }
        }

        private long extractBloomFilterIndexSizeInBytesOption(
                        final CommandLine cmd) {
                if (cmd.hasOption("bloom-filter-index-size-in-bytes")) {
                        return parseLong(cmd.getOptionValue("bloom-filter-index-size-in-bytes"));
                } else {
                        throw new IllegalArgumentException(
                                        "When you select this task then you must specify bloom filter index size in bytes");
                }
        }

        private long extractBloomFilterNumberOfHashFunctionsOption(
                        final CommandLine cmd) {
                if (cmd.hasOption("bloom-filter-number-of-hash-functions")) {
                        return parseLong(cmd.getOptionValue("bloom-filter-number-of-hash-functions"));
                } else {
                        throw new IllegalArgumentException(
                                        "When you select this task then you must specify bloom filter number of hash functions");
                }
        }

        private String extractDirectoryOption(final CommandLine cmd) {
                if (cmd.hasOption(OPTION_DIRECTORY)) {
                        return cmd.getOptionValue(OPTION_DIRECTORY);
                } else {
                        throw new IllegalArgumentException(
                                        "When you select count or search task then you must specify directory");
                }
        }

        private long parseLong(final String str) {
                Objects.requireNonNull(str);
                final String tmp = str.replace("_", "").replace("L", "")
                                .replace("l", "");
                final long out = Long.parseLong(tmp);
                return out;
        }

}
