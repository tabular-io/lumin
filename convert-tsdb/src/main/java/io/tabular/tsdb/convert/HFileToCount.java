package io.tabular.tsdb.convert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.spark.api.java.function.Function;

class HFileToCount implements Function<String, Long> {

  private final ConfigHolder configHolder;

  HFileToCount(ConfigHolder configHolder) {
    this.configHolder = configHolder;
  }

  @Override
  public Long call(String file) throws Exception {
    Configuration config = configHolder.getConfig();
    Path path = new Path(file);
    FileSystem fs = path.getFileSystem(config);
    try (HFile.Reader reader = HFile.createReader(fs, path, config)) {
      return reader.getEntries();
    }
  }
}
