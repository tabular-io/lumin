package io.tabular.tsdb.convert;

import io.tabular.tsdb.convert.model.CellData;
import java.util.Iterator;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.sparkproject.guava.collect.Iterators;

class HFileToCellData implements FlatMapFunction<String, CellData> {

  private final ConfigHolder configHolder;

  HFileToCellData(ConfigHolder configHolder) {
    this.configHolder = configHolder;
  }

  @Override
  public Iterator<CellData> call(String file) throws Exception {
    Configuration config = configHolder.getConfig();
    Path path = new Path(file);
    FileSystem fs = path.getFileSystem(config);

    if (!fs.exists(path)) {
      // File may have been deleted since the job started, e.g. if running against a live
      // Hbase cluster and a compaction occurred
      return Iterators.emptyIterator();
    }

    HFile.Reader reader = HFile.createReader(fs, path, config);
    HFileScanner scanner = reader.getScanner(false, false);

    if (!scanner.seekTo()) {
      reader.close();
      return Iterators.emptyIterator();
    }

    return new Iterator<CellData>() {
      private boolean hasNext = true;

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      @SneakyThrows
      public CellData next() {
        CellData value = new CellData(scanner.getCell(), file);

        hasNext = scanner.next();

        if (!hasNext) {
          reader.close();
        }

        return value;
      }
    };
  }
}
