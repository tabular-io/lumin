package io.tabular.tsdb.convert.model;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class UID implements Serializable {
  private final byte[] uid;
  private final String qualifier;
  private final String name;

  public static UID fromCellData(CellData cellData) {
    String family = new String(cellData.getFamily(), StandardCharsets.UTF_8);
    if (!"name".equals(family)) {
      return null;
    }

    byte[] uid = cellData.getRowKey();
    String qualifier = new String(cellData.getQualifier(), StandardCharsets.UTF_8);
    String name = new String(cellData.getValue(), StandardCharsets.UTF_8);

    return new UID(uid, qualifier, name);
  }
}
