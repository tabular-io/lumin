package io.tabular.tsdb.convert;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Main {

  private static final int DEFAULT_ID_SIZE = 4;

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption("m", "metric-dir", true, "Directory containing metric data files");
    options.addOption("u", "uid-dir", true, "Directory containing UID data files");
    options.addOption("o", "output-table", true, "Output table");
    options.addOption("z", "size-of-id", true, "Size of ID fields, in bytes (default: 4)");
    options.addOption("f", "fanout", false, "Enable fanout write instead of sorting");
    options.addOption("y", "dry-run", false, "Run process without writing to table");
    options.addOption("g", "limit-input-gb", false, "Limit input in GB, for testing");

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    ConvertOptions convertOptions =
        ConvertOptions.builder()
            .metricDir(cmd.getOptionValue("m"))
            .uidDir(cmd.getOptionValue("u"))
            .outputTable(cmd.getOptionValue("o"))
            .idSize(
                cmd.hasOption("z") ? Integer.parseInt(cmd.getOptionValue("z")) : DEFAULT_ID_SIZE)
            .fanout(cmd.hasOption("f"))
            .dryRun(cmd.hasOption("y"))
            .limitGb(cmd.hasOption("g") ? Integer.parseInt(cmd.getOptionValue("g")) : 0)
            .build();

    SparkConf conf = new SparkConf().setAppName("tsdb-import");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

    new Convert(spark, convertOptions).convert();
  }
}
