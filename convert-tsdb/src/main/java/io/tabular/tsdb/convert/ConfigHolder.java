// Copyright 2022 Tabular Technologies Inc.
package io.tabular.tsdb.convert;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;

@AllArgsConstructor
@Getter
public class ConfigHolder implements Serializable {
  private Configuration config;

  @SneakyThrows
  private void readObject(ObjectInputStream inputStream) {
    config = new Configuration(false);
    config.readFields(inputStream);
  }

  @SneakyThrows
  private void writeObject(ObjectOutputStream outputStream) {
    config.write(outputStream);
  }
}
