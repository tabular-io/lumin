package lumin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class CellParser {

  public static class Metric {
    private static final int PREFIX_BYTES = 11;
    private static final int SALT_BYTES = 4;
    private static final int UID_BYTES = 3;
    private static final int TS_BYTES = 4;

    byte [] salt = new byte[SALT_BYTES];
    byte [] muid = new byte [UID_BYTES];
    byte [] ts = new byte[8];
    Map<byte[], byte[]> tags = Maps.newHashMap();
    byte [] qualifier;
    byte [] value;

    public Metric(Cell cell) {
      byte [] data = cell.getRowArray();

      System.arraycopy(data, 0, salt, 0, SALT_BYTES);
      System.arraycopy(data, SALT_BYTES, muid, 0,UID_BYTES);
      System.arraycopy(data, SALT_BYTES + UID_BYTES, ts, 4,TS_BYTES);

      // Validate that tag k/v ids are as expected
      Preconditions.checkArgument((data.length - PREFIX_BYTES) % 3 == 0);

      int tagCount = (cell.getRowLength() - PREFIX_BYTES) / 6;
      int pos = PREFIX_BYTES;

      for (int i = 0; i < tagCount; i++) {
        byte [] key = new byte[UID_BYTES];
        byte [] value = new byte[UID_BYTES];

        System.arraycopy(data, pos, key, 0, 3);
        pos += UID_BYTES;
        System.arraycopy(data, pos, value, 0, 3);
        pos += UID_BYTES;

        tags.put(key, value);
      }

      this.qualifier = CellUtil.cloneQualifier(cell);
      this.value = CellUtil.cloneValue(cell);
    }
  }

  public static void parse(Cell cell) {

  }

}
