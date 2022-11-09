package io.tabular.tsdb.convert;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.days;

import io.tabular.tsdb.convert.model.CellData;
import io.tabular.tsdb.convert.model.Metric;
import io.tabular.tsdb.convert.model.UID;
import java.io.Serializable;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

public class Convert implements Serializable {

  private static final int DEFAULT_ID_SIZE = 4;

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final ConvertOptions convertOptions;

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption("m", "metric-dir", true, "Directory containing metric data files");
    options.addOption("u", "uid-dir", true, "Directory containing UID data files");
    options.addOption("o", "output-table", true, "Output table");
    options.addOption("z", "size-of-id", true, "Size of ID fields, in bytes (default: 4)");
    options.addOption("f", "fanout", false, "Enable fanout write instead of sorting");
    options.addOption("y", "dry-run", false, "Run process without writing to table");
    options.addOption("g", "limit-input-gb", true, "Limit input in GB, for testing");

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

  public Convert(SparkSession spark, ConvertOptions convertOptions) {
    this.spark = spark;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.convertOptions = convertOptions;
  }

  public void convert() {
    List<UID> uidList = mapUidHFiles(convertOptions.getUidDir(), UID::fromCellData).collect();
    Broadcast<List<UID>> uidBroadcast = sparkContext.broadcast(uidList);

    JavaRDD<Metric> metricRdd =
        flatMapMetricHFiles(
            convertOptions.getMetricDir(),
            new MetricMapFunction(uidBroadcast, convertOptions.getIdSize()));
    writeOutput(metricRdd);
  }

  private void writeOutput(JavaRDD<Metric> metricRdd) {
    Dataset<Row> df =
        spark.createDataset(metricRdd.map(Metric::toRow).rdd(), RowEncoder.apply(Metric.SCHEMA));
    if (convertOptions.isDryRun()) {
      System.out.println("*** Row count: " + df.count());
    } else if (convertOptions.isFanout()) {
      df.writeTo(convertOptions.getOutputTable())
          .partitionedBy(days(col("time")))
          .option("fanout-enabled", true)
          .createOrReplace();
    } else {
      df.orderBy(col("time"))
          .writeTo(convertOptions.getOutputTable())
          .partitionedBy(days(col("time")))
          .createOrReplace();
    }
  }

  private <T> JavaRDD<T> mapUidHFiles(String sourceDir, Function<CellData, T> fn) {
    return createRDD(sourceDir, 0).map(fn).filter(Objects::nonNull);
  }

  private <T> JavaRDD<T> flatMapMetricHFiles(String sourceDir, FlatMapFunction<CellData, T> fn) {
    return createRDD(sourceDir, convertOptions.getLimitGb()).flatMap(fn).filter(Objects::nonNull);
  }

  private JavaRDD<CellData> createRDD(String sourceDir, int limitGb) {
    List<String> files = Utilities.sourceFiles(sparkContext, sourceDir, limitGb);
    return sparkContext
        .parallelize(files, files.size())
        .flatMap(new HFileToCellData(new ConfigHolder(sparkContext.hadoopConfiguration())));
  }
}
