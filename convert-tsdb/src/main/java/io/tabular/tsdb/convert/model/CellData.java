package io.tabular.tsdb.convert.model;

import java.io.Serializable;
import lombok.Getter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

@Getter
public class CellData implements Serializable {
  private byte[] rowKey;
  private byte[] family;
  private byte[] qualifier;
  private byte[] value;

  public CellData(Cell cell) {
    this.rowKey = CellUtil.cloneRow(cell);
    this.family = CellUtil.cloneFamily(cell);
    this.qualifier = CellUtil.cloneQualifier(cell);
    this.value = CellUtil.cloneValue(cell);
  }
}
