package lumin;

import com.google.common.collect.Maps;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
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
              + "tags MAP<STRING, STRING>, "
              + "value DOUBLE, "
              + "ts TIMESTAMP, "
              + "row_key BINARY, "
              + "qualifier BINARY, "
              + "value_bytes BINARY");

  private static final int SALT_BYTES = 1;
  private static final int UID_BYTES = 4;
  private static final int TS_BYTES = 4;
  private static final int PREFIX_BYTES = SALT_BYTES + UID_BYTES + TS_BYTES;
  private static final int TAG_BYTES = 4;

  String metricName;
  Map<String, String> tags;
  double value;
  Timestamp ts;
  byte[] rowKey;
  byte[] qualifier;
  byte[] valueBytes;

  public Metric(
      String metricName,
      Map<String, String> tags,
      double value,
      Timestamp ts,
      byte[] rowKey,
      byte[] qualifier,
      byte[] valueBytes) {
    this.metricName = metricName;
    this.tags = tags;
    this.value = value;
    this.ts = ts;
    this.rowKey = rowKey;
    this.qualifier = qualifier;
    this.valueBytes = valueBytes;
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
    long millis = ByteBuffer.wrap(tsBytes).getInt() * 1000L;

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

    byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
    if (qualifierBytes.length != 2) {
      throw new RuntimeException("Unsupported qualifier format");
    }

    // first 12 bits are seconds
    short qualifier = ByteBuffer.wrap(qualifierBytes).getShort();
    int offsetSec = qualifier >> 4;
    millis += (1000L * offsetSec);
    Timestamp ts = new Timestamp(millis);

    byte[] valueBytes = CellUtil.cloneValue(cell);

    // last 4 bits are value format
    double value;
    if ((qualifier & 0b1111) == 0b1111) {
      value = ByteBuffer.wrap(valueBytes).getDouble();
    } else if ((qualifier & 0b1111) == 0 && valueBytes[0] == 0) {
      value = 0;
    } else {
      throw new RuntimeException("Unexpected value type, expecting double");
    }

    return new Metric(metricName, tags, value, ts, rowKey, qualifierBytes, valueBytes);
  }

  public Row toRow() {
    return RowFactory.create(
        metricName, JavaConverters.mapAsScalaMap(tags), value, ts, rowKey, qualifier, valueBytes);
  }
}
