package com.coroptis.index.loadtest;

import java.util.Objects;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class LoadTestCli {

        private final static Option OPTION_HELP = new Option("h", "help", false,
                        "display help");
        private final static Option OPTION_DIRECTORY = new Option("d",
                        "directory", true,
                        "directory " + "where indexe lies, when user "
                                        + "select count or search task "
                                        + "then this parameter"
                                        + " is mandatory");

        // Write and related parameters
        private final static Option OPTION_WRITE = new Option("w", "write",
                        false, "write new random data into index.");
        private final static Option OPTION_COUNT = Option.builder("c")//
                        .longOpt("count")//
                        .hasArg(false)//
                        .required(false)//
                        .desc("How many key will be written").build();

        // Search and related parameters
        private final static Option OPTION_SEARCH = new Option("s", "search",
                        false, "search random data from index.");

        private final static Option OPTION_MAX_KEY = Option.builder("m")//
                        .longOpt("max-key")//
                        .hasArg(false)//
                        .required(false)//
                        .desc("Max key value to search").build();

        // commmon index parameters

        private final static Option OPTION_CUSTOM_CONF = Option.builder("b")//
                        .longOpt("custom-conf")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("Build with custom configuration, default is true")
                        .build();

        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT = Option
                        .builder("f")//
                        .longOpt("MaxNumberOfKeysInSegment")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("MaxNumberOfKeysInSegment").build();
        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = Option
                        .builder("f")//
                        .longOpt("MaxNumberOfKeysInSegmentCache")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("MaxNumberOfKeysInSegmentCache").build();
        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = Option
                        .builder("f")//
                        .longOpt("MaxNumberOfKeysInSegmentCacheDuringFlushing")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("MaxNumberOfKeysInSegmentCacheDuringFlushing")
                        .build();
        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE = Option
                        .builder("f")//
                        .longOpt("MaxNumberOfKeysInSegmentIndexPage")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("MaxNumberOfKeysInSegmentIndexPage").build();
        private final static Option OPTION_MAX_NUMBER_OF_KEYS_IN_CACHE = Option
                        .builder("f")//
                        .longOpt("MaxNumberOfKeysInCache")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("MaxNumberOfKeysInCache").build();
        private final static Option OPTION_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = Option
                        .builder("f")//
                        .longOpt("BloomFilterIndexSizeInBytes")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("BloomFilterIndexSizeInBytes").build();
        private final static Option OPTION_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = Option
                        .builder("f")//
                        .longOpt("BloomFilterNumberOfHashFunctions")//
                        .hasArg(true)//
                        .required(false)//
                        .desc("BloomFilterNumberOfHashFunctions").build();

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
                options.addOption(OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING);
                options.addOption(OPTION_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE);
                options.addOption(OPTION_MAX_NUMBER_OF_KEYS_IN_CACHE);
                options.addOption(OPTION_BLOOM_FILTER_INDEX_SIZE_IN_BYTES);
                options.addOption(OPTION_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS);

                final CommandLineParser parser = new DefaultParser();
                final CommandLine cmd = parser.parse(options, args);
                if (cmd.hasOption(OPTION_HELP)) {
                        final HelpFormatter formatter = new HelpFormatter();
                        formatter.printHelp("./run", options);
                } else {
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
