package io.tabular.tsdb.convert;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.hours;

import com.google.common.collect.Maps;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkContext;
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
  private final String metricDir;
  private final String uidDir;
  private final String outputTable;
  private final int idSize;
  private boolean fanout;

  public Convert(
      SparkSession spark,
      String metricDir,
      String uidDir,
      String outputTable,
      int idSize,
      boolean fanout) {
    this.spark = spark;
    this.metricDir = metricDir;
    this.uidDir = uidDir;
    this.outputTable = outputTable;
    this.idSize = idSize;
    this.fanout = fanout;
  }

  public void convert() {
    List<UID> uidList = mapHFiles(uidDir, UID::fromCellData).collect();
    Broadcast<List<UID>> uidBroadcast =
        new JavaSparkContext(spark.sparkContext()).broadcast(uidList);

    JavaRDD<Metric> metricRdd =
        flatMapHFiles(metricDir, new MetricMapFunction(uidBroadcast, idSize));
    writeOutput(metricRdd);
  }

  private void writeOutput(JavaRDD<Metric> metricRdd) {
    Dataset<Row> df =
        spark.createDataset(metricRdd.map(Metric::toRow).rdd(), RowEncoder.apply(Metric.SCHEMA));
    if (fanout) {
      df.writeTo(outputTable)
          .partitionedBy(hours(col("ts")))
          .option("fanout-enabled", true)
          .createOrReplace();
    } else {
      df.orderBy(col("ts")).writeTo(outputTable).partitionedBy(hours(col("ts"))).createOrReplace();
    }
  }

  private <T> JavaRDD<T> mapHFiles(String sourceDir, Function<CellData, T> fn) {
    return createRDD(sourceDir).map(fn).filter(Objects::nonNull);
  }

  private <T> JavaRDD<T> flatMapHFiles(String sourceDir, FlatMapFunction<CellData, T> fn) {
    return createRDD(sourceDir).flatMap(fn).filter(Objects::nonNull);
  }

  private JavaRDD<CellData> createRDD(String sourceDir) {
    SparkContext ctx = spark.sparkContext();
    return ctx.newAPIHadoopFile(
            sourceDir,
            HFileInputFormat.class,
            NullWritable.class,
            Cell.class,
            ctx.hadoopConfiguration())
        .toJavaRDD()
        .map(tuple -> new CellData(tuple._2));
  }

  static class MetricMapFunction implements FlatMapFunction<CellData, Metric> {
    private final Broadcast<List<UID>> uidBroadcast;
    private final int idSize;
    private transient Map<ByteBuffer, String> metricMap;
    private transient Map<ByteBuffer, String> tagKeyMap;
    private transient Map<ByteBuffer, String> tagValueMap;

    MetricMapFunction(Broadcast<List<UID>> uidBroadcast, int idSize) {
      this.uidBroadcast = uidBroadcast;
      this.idSize = idSize;
    }

    @Override
    public Iterator<Metric> call(CellData cellData) {
      if (metricMap == null) {
        loadMaps(uidBroadcast.value());
      }
      return Metric.fromCellData(cellData, metricMap, tagKeyMap, tagValueMap, idSize).iterator();
    }

    private void loadMaps(List<UID> uidList) {
      metricMap = Maps.newHashMap();
      tagKeyMap = Maps.newHashMap();
      tagValueMap = Maps.newHashMap();
      for (UID uid : uidList) {
        switch (uid.qualifier) {
          case "metrics":
            metricMap.put(ByteBuffer.wrap(uid.uid), uid.name);
            break;
          case "tagk":
            tagKeyMap.put(ByteBuffer.wrap(uid.uid), uid.name);
            break;
          case "tagv":
            tagValueMap.put(ByteBuffer.wrap(uid.uid), uid.name);
            break;
        }
      }
    }
  }
}
