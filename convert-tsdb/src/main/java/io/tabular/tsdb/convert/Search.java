package io.tabular.tsdb.convert;

import io.tabular.tsdb.convert.model.CellData;
import io.tabular.tsdb.convert.model.Metric;
import io.tabular.tsdb.convert.model.UID;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class Search implements Serializable {

  private static final int DEFAULT_ID_SIZE = 4;

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final SearchOptions searchOptions;

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption("m", "metric-dir", true, "Directory containing metric data files");
    options.addOption("u", "uid-dir", true, "Directory containing UID data files");
    options.addOption("z", "size-of-id", true, "Size of ID fields, in bytes (default: 4)");

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    SearchOptions searchOptions =
        SearchOptions.builder()
            .metricDir(cmd.getOptionValue("m"))
            .uidDir(cmd.getOptionValue("u"))
            .idSize(
                cmd.hasOption("z") ? Integer.parseInt(cmd.getOptionValue("z")) : DEFAULT_ID_SIZE)
            .build();

    SparkConf conf = new SparkConf().setAppName("tsdb-search");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

    new Search(spark, searchOptions).convert();
  }

  public Search(SparkSession spark, SearchOptions searchOptions) {
    this.spark = spark;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.searchOptions = searchOptions;
  }

  public void convert() {
    List<UID> uidList = mapUidHFiles(searchOptions.getUidDir(), UID::fromCellData).collect();
    Broadcast<List<UID>> uidBroadcast = sparkContext.broadcast(uidList);

    JavaRDD<Metric> metricRdd =
        flatMapMetricHFiles(
            searchOptions.getMetricDir(),
            new MetricMapFunction(uidBroadcast, searchOptions.getIdSize()));

    // TODO: make these config options
    String metricFilter = "power";
    Integer lspId = 2400;
    long start = Instant.parse("2021-04-02T10:50:00Z").toEpochMilli();
    long end = Instant.parse("2021-04-03T10:50:00Z").toEpochMilli();

    List<String> foundInFiles =
        metricRdd
            .filter(
                metric ->
                    metricFilter.equals(metric.getMetricName())
                        && lspId.equals(metric.getLspId())
                        && metric.getTs().getTime() >= start
                        && metric.getTs().getTime() <= end)
            .map(Metric::getFile)
            .distinct()
            .collect();

    System.out.println("***** Found in files:");
    foundInFiles.forEach(System.out::println);
  }

  private <T> JavaRDD<T> mapUidHFiles(String sourceDir, Function<CellData, T> fn) {
    return createRDD(sourceDir).map(fn).filter(Objects::nonNull);
  }

  private <T> JavaRDD<T> flatMapMetricHFiles(String sourceDir, FlatMapFunction<CellData, T> fn) {
    return createRDD(sourceDir).flatMap(fn).filter(Objects::nonNull);
  }

  private JavaRDD<CellData> createRDD(String sourceDir) {
    List<String> files = Utilities.sourceFiles(sparkContext, sourceDir, 0);
    return sparkContext
        .parallelize(files, files.size())
        .flatMap(new HFileToCellData(new ConfigHolder(sparkContext.hadoopConfiguration())));
  }
}
