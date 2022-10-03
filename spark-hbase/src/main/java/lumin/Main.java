package lumin;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Main {

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption("m", "metrics-dir", true, "Directory containing metrics data files");
    options.addOption("M", "metrics-table", true, "Output table for metrics data");
    options.addOption("u", "uid-dir", true, "Directory containing UID data files");
    options.addOption("U", "uid-table", true, "Output table for UID data");

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    String metricsDir = cmd.getOptionValue("m");
    String metricsTable = cmd.getOptionValue("M");
    String uidDir = cmd.getOptionValue("u");
    String uidTable = cmd.getOptionValue("U");

    SparkConf conf = new SparkConf().setAppName("tsdb-import");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

    new Convert(spark, metricsDir, metricsTable, uidDir, uidTable).convert();
  }
}
