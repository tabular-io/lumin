package io.tabular.tsdb.convert;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.hours;

import com.google.common.collect.Lists;
import io.tabular.tsdb.convert.model.CellData;
import io.tabular.tsdb.convert.model.Metric;
import io.tabular.tsdb.convert.model.UID;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final ConvertOptions convertOptions;

  private static final long GB_BYTES = 1_000_000_000;

  public Convert(SparkSession spark, ConvertOptions convertOptions) {
    this.spark = spark;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.convertOptions = convertOptions;
  }

  public void convert() {
    List<UID> uidList = mapHFiles(convertOptions.getUidDir(), UID::fromCellData).collect();
    Broadcast<List<UID>> uidBroadcast = sparkContext.broadcast(uidList);

    JavaRDD<Metric> metricRdd =
        flatMapHFiles(
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
          .partitionedBy(hours(col("ts")))
          .option("fanout-enabled", true)
          .createOrReplace();
    } else {
      df.orderBy(col("ts"))
          .writeTo(convertOptions.getOutputTable())
          .partitionedBy(hours(col("ts")))
          .createOrReplace();
    }
  }

  private <T> JavaRDD<T> mapHFiles(String sourceDir, Function<CellData, T> fn) {
    return createRDD(sourceDir).map(fn).filter(Objects::nonNull);
  }

  private <T> JavaRDD<T> flatMapHFiles(String sourceDir, FlatMapFunction<CellData, T> fn) {
    return createRDD(sourceDir).flatMap(fn).filter(Objects::nonNull);
  }

  private JavaRDD<CellData> createRDD(String sourceDir) {
    List<String> files = sourceFiles(sourceDir);
    return sparkContext
        .parallelize(files, files.size())
        .flatMap(new HFileToCellData(new ConfigHolder(spark.sparkContext().hadoopConfiguration())));
  }

  private List<String> sourceFiles(String sourceDir) {
    try {
      Path path = new Path(sourceDir);
      FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());

      List<String> result = Lists.newArrayList();
      long size = 0;
      RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, true);
      while (fileStatusListIterator.hasNext()) {
        LocatedFileStatus fileStatus = fileStatusListIterator.next();
        String name = fileStatus.getPath().getName();

        // this will filter out non-HFiles with names such as .tableinfo, .regioninfo, xxxx.yyyy
        if (fileStatus.isFile() && !name.contains(".") && !name.endsWith("_$folder$")) {
          result.add(fileStatus.getPath().toString());
          size += fileStatus.getLen();
        }

        if (convertOptions.getLimitGb() > 0 && size > convertOptions.getLimitGb() * GB_BYTES) {
          break;
        }
      }
      return result;
    } catch (IOException x) {
      throw new RuntimeException(x);
    }
  }
}
