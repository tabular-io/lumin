package lumin;

import com.google.common.collect.Maps;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class Metric implements Serializable {

  public static final StructType SCHEMA =
      StructType.fromDDL(
          "metric_name STRING, "
              + "ts BIGINT, "
              + "tags MAP<STRING, STRING>, "
              + "row_key BINARY, "
              + "qualifier BINARY, "
              + "value BINARY");

  private static final int SALT_BYTES = 1;
  private static final int UID_BYTES = 4;
  private static final int TS_BYTES = 4;
  private static final int PREFIX_BYTES = SALT_BYTES + UID_BYTES + TS_BYTES;
  private static final int TAG_BYTES = 4;

  String metricName;
  long ts;
  Map<String, String> tags;
  byte[] rowKey;
  byte[] qualifier;
  byte[] value;

  public Metric(
      String metricName,
      long ts,
      Map<String, String> tags,
      byte[] rowKey,
      byte[] qualifier,
      byte[] value) {
    this.metricName = metricName;
    this.ts = ts;
    this.tags = tags;
    this.rowKey = rowKey;
    this.qualifier = qualifier;
    this.value = value;
  }

  public static Metric fromCell(
      Cell cell,
      Map<ByteBuffer, String> metricMap,
      Map<ByteBuffer, String> tagKeyMap,
      Map<ByteBuffer, String> tagValueMap) {

    byte[] rowKey = CellUtil.cloneRow(cell);

    // Validate size for prefix and tag k/v ids
    if (rowKey.length < PREFIX_BYTES || (rowKey.length - PREFIX_BYTES) % (TAG_BYTES * 2) != 0) {
      throw new RuntimeException("Unsupported row key format");
    }

    byte[] metricId = new byte[UID_BYTES];
    System.arraycopy(rowKey, SALT_BYTES, metricId, 0, UID_BYTES);
    String metricName = metricMap.get(ByteBuffer.wrap(metricId));
    if (metricName == null) {
      throw new RuntimeException("Unable to map metric ID to name");
    }

    byte[] tsBytes = new byte[TS_BYTES];
    System.arraycopy(rowKey, SALT_BYTES + UID_BYTES, tsBytes, 0, TS_BYTES);
    long ts = ByteBuffer.wrap(tsBytes).getInt() * 1000L;

    int tagCount = (rowKey.length - PREFIX_BYTES) / (TAG_BYTES * 2);
    int pos = PREFIX_BYTES;

    Map<byte[], byte[]> tagIds = Maps.newHashMap();
    Map<String, String> tags = Maps.newHashMap();
    for (int i = 0; i < tagCount; i++) {
      byte[] tagk = new byte[TAG_BYTES];
      byte[] tagv = new byte[TAG_BYTES];

      tagIds.put(tagk, tagv);

      System.arraycopy(rowKey, pos, tagk, 0, TAG_BYTES);
      pos += TAG_BYTES;
      System.arraycopy(rowKey, pos, tagv, 0, TAG_BYTES);
      pos += TAG_BYTES;

      String tagkStr = tagKeyMap.get(ByteBuffer.wrap(tagk));
      if (tagkStr == null) {
        throw new RuntimeException("Unable to map tag key to name");
      }

      String tagvStr = tagValueMap.get(ByteBuffer.wrap(tagv));
      if (tagvStr == null) {
        throw new RuntimeException("Unable to map tag value to name");
      }

      tags.put(tagkStr, tagvStr);
    }

    byte[] qualifier = CellUtil.cloneQualifier(cell);
    if (qualifier.length == 2) {
      // first 12 bits are seconds
      int offsetSec = ByteBuffer.wrap(qualifier).getShort() >> 4;
      ts += (1000L * offsetSec);
    } else {
      throw new RuntimeException("Unsupported qualifier format");
    }
    byte[] value = CellUtil.cloneValue(cell);

    return new Metric(metricName, ts, tags, rowKey, qualifier, value);
  }

  public Row toRow() {
    return RowFactory.create(
        metricName, ts, JavaConverters.mapAsScalaMap(tags), rowKey, qualifier, value);
  }
}
