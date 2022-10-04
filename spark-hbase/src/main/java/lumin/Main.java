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

    options.addOption("m", "metric-dir", true, "Directory containing metric data files");
    options.addOption("u", "uid-dir", true, "Directory containing UID data files");
    options.addOption("o", "output-table", true, "Output table");

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    String metricDir = cmd.getOptionValue("m");
    String uidDir = cmd.getOptionValue("u");
    String outputTable = cmd.getOptionValue("o");

    SparkConf conf = new SparkConf().setAppName("tsdb-import");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

    new Convert(spark, metricDir, uidDir, outputTable).convert();
  }
}
