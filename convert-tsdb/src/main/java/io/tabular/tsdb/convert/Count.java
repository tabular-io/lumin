package io.tabular.tsdb.convert;

import java.io.Serializable;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Count implements Serializable {

  private final JavaSparkContext sparkContext;
  private final String metricDir;

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption("m", "metric-dir", true, "Directory containing metric data files");

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    String dir = cmd.getOptionValue("m");

    SparkConf conf = new SparkConf().setAppName("tsdb-count");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

    long cnt = new Count(spark, dir).count();

    System.out.println("******* Record count: " + cnt);
  }

  public Count(SparkSession spark, String metricDir) {
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.metricDir = metricDir;
  }

  public long count() {
    List<String> files = Utilities.sourceFiles(sparkContext, metricDir, 0);
    return sparkContext
        .parallelize(files, files.size())
        .map(new HFileToCount(new ConfigHolder(sparkContext.hadoopConfiguration())))
        .reduce(Long::sum);
  }
}
