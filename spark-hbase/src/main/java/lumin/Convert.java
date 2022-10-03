package lumin;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

public class Convert implements Serializable {

  private SparkSession spark;
  private String metricDir;
  private String metricTable;
  private String uidDir;
  private String uidTable;

  private Broadcast<List<UID>> uidBroadcast;

  public Convert(
      SparkSession spark, String metricDir, String metricTable, String uidDir, String uidTable) {
    this.spark = spark;
    this.metricDir = metricDir;
    this.metricTable = metricTable;
    this.uidDir = uidDir;
    this.uidTable = uidTable;
  }

  public void convert() {
    JavaRDD<UID> uidRdd = loadHFiles(spark, uidDir, UID::fromCell);
    writeUidTable(uidRdd);

    List<UID> uidList = uidRdd.collect();
    uidBroadcast = new JavaSparkContext(spark.sparkContext()).broadcast(uidList);

    JavaRDD<Metric> metricRdd = loadHFiles(spark, metricDir, new MetricMapFunction(uidBroadcast));
    writeTsdb(metricRdd);
  }

  private void writeUidTable(JavaRDD<UID> uidRdd) {
    spark
        .createDataset(uidRdd.map(UID::toRow).rdd(), RowEncoder.apply(UID.SCHEMA))
        .writeTo(uidTable)
        .createOrReplace();
  }

  private void writeTsdb(JavaRDD<Metric> metricRdd) {
    spark
        .createDataset(metricRdd.map(Metric::toRow).rdd(), RowEncoder.apply(Metric.SCHEMA))
        .writeTo(metricTable)
        .createOrReplace();
  }

  private <T> JavaRDD<T> loadHFiles(SparkSession spark, String sourceDir, MapFunction<Cell, T> fn) {
    SparkContext ctx = spark.sparkContext();
    return ctx.newAPIHadoopFile(
            sourceDir,
            HFileInputFormat.class,
            NullWritable.class,
            Cell.class,
            ctx.hadoopConfiguration())
        .toJavaRDD()
        .map(tuple -> fn.call(tuple._2))
        .filter(Objects::nonNull);
  }

  static class MetricMapFunction implements MapFunction<Cell, Metric> {
    private final Broadcast<List<UID>> uidBroadcast;
    private transient Map<ByteBuffer, String> metricMap;
    private transient Map<ByteBuffer, String> tagKeyMap;
    private transient Map<ByteBuffer, String> tagValueMap;

    MetricMapFunction(Broadcast<List<UID>> uidBroadcast) {
      this.uidBroadcast = uidBroadcast;
    }

    @Override
    public Metric call(Cell cell) {
      if (metricMap == null) {
        loadMaps(uidBroadcast.value());
      }
      return Metric.fromCell(cell, metricMap, tagKeyMap, tagValueMap);
    }

    private void loadMaps(List<UID> uidList) {
      metricMap = new HashMap<>();
      tagKeyMap = new HashMap<>();
      tagValueMap = new HashMap<>();
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
