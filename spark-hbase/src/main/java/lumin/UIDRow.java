package lumin;

import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class UIDRow {
  byte[] uid;
  byte[] qualifier;
  String name;

  public UIDRow(Cell cell) {
    String family = new String(CellUtil.cloneFamily(cell), StandardCharsets.UTF_8);
    if (!"name".equals(family)) {
      return;
    }

    this.uid = CellUtil.cloneRow(cell);
    this.qualifier = CellUtil.cloneQualifier(cell);
    this.name = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
  }
}
