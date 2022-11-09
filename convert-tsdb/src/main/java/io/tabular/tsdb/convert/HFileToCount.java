package io.tabular.tsdb.convert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
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
      HFileScanner scanner = reader.getScanner(false, false);
      long valCnt = 0;
      if (scanner.seekTo()) {
        do {
          int qualifierLen = scanner.getCell().getQualifierLength();
          // only count data points, so ignore qualifier len of 3 and 5
          if (qualifierLen != 3 && qualifierLen != 5) {
            valCnt += (qualifierLen / 2);
          }
        } while (scanner.next());
      }
      return valCnt;
    }
  }
}
