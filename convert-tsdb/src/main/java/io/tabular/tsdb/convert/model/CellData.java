package io.tabular.tsdb.convert.model;

import java.io.Serializable;
import lombok.Getter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

@Getter
public class CellData implements Serializable {
  private final byte[] rowKey;
  private final byte[] family;
  private final byte[] qualifier;
  private final byte[] value;
  private final String file;
  private long version;

  public CellData(Cell cell, String file) {
    this.rowKey = CellUtil.cloneRow(cell);
    this.family = CellUtil.cloneFamily(cell);
    this.qualifier = CellUtil.cloneQualifier(cell);
    this.value = CellUtil.cloneValue(cell);
    this.version = cell.getTimestamp();
    this.file = file;
  }
}
