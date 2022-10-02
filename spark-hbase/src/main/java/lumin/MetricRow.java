package lumin;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class MetricRow {

  public static final StructType SCHMEA =
      StructType.fromDDL(
          "salt BINARY, metric_id BINARY, ts BINARY, tags MAP<BINARY, BINARY>, qualifier BINARY, value BINARY");

  private static final int SALT_BYTES = 4;
  private static final int UID_BYTES = 3;
  private static final int TS_BYTES = 4;
  private static final int PREFIX_BYTES = SALT_BYTES + UID_BYTES + TS_BYTES;
  private static final int TAG_BYTES = 3;

  private byte[] salt = new byte[SALT_BYTES];
  private byte[] muid = new byte[UID_BYTES];
  private byte[] ts = new byte[8];
  private Map<byte[], byte[]> tags = Maps.newHashMap();
  private byte[] qualifier;
  private byte[] value;

  public MetricRow(Cell cell) {
    byte[] data = cell.getRowArray();
    int dataIdx = cell.getRowOffset();
    int dataLen = cell.getRowLength();

    // Validate size for prefix and tag k/v ids
    if (dataLen < PREFIX_BYTES || (dataLen - PREFIX_BYTES) % (TAG_BYTES * 2) != 0) {
      // TODO: handle error
      return;
    }

    System.arraycopy(data, dataIdx, salt, 0, SALT_BYTES);
    System.arraycopy(data, dataIdx + SALT_BYTES, muid, 0, UID_BYTES);
    System.arraycopy(data, dataIdx + SALT_BYTES + UID_BYTES, ts, ts.length - TS_BYTES, TS_BYTES);

    int tagCount = (dataLen - PREFIX_BYTES) / (TAG_BYTES * 2);
    int pos = dataIdx + PREFIX_BYTES;

    for (int i = 0; i < tagCount; i++) {
      byte[] key = new byte[TAG_BYTES];
      byte[] value = new byte[TAG_BYTES];

      System.arraycopy(data, pos, key, 0, TAG_BYTES);
      pos += TAG_BYTES;
      System.arraycopy(data, pos, value, 0, TAG_BYTES);
      pos += TAG_BYTES;

      tags.put(key, value);
    }

    this.qualifier = CellUtil.cloneQualifier(cell);
    this.value = CellUtil.cloneValue(cell);
  }

  public Row toRow() {
    return RowFactory.create(salt, muid, ts, JavaConverters.mapAsScalaMap(tags), qualifier, value);
  }
}
