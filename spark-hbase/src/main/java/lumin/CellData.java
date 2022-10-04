package lumin;

import java.io.Serializable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class CellData implements Serializable {
  byte[] rowKey;
  byte[] family;
  byte[] qualifier;
  byte[] value;

  public CellData(Cell cell) {
    this.rowKey = CellUtil.cloneRow(cell);
    this.family = CellUtil.cloneFamily(cell);
    this.qualifier = CellUtil.cloneQualifier(cell);
    this.value = CellUtil.cloneValue(cell);
  }
}
