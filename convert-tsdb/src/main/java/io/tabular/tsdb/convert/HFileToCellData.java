package io.tabular.tsdb.convert;

import io.tabular.tsdb.convert.model.CellData;
import java.time.Instant;
import java.util.Iterator;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.sparkproject.guava.collect.Iterators;

class HFileToCellData implements FlatMapFunction<String, CellData> {

  private static final int SALT_SIZE = 1;
  private static final int ID_SIZE = 4; // TODO: get config
  private static final int SKIP = SALT_SIZE + ID_SIZE;
  private static final int TS_SIZE = 4;

  public static final long MIN_DATE = Instant.parse("2022-12-01T00:00:00.00Z").toEpochMilli();
  public static final long MAX_DATE = Instant.parse("2022-12-15T00:00:00.00Z").toEpochMilli();

  private final ConfigHolder configHolder;
  private final boolean dateFilter;

  HFileToCellData(boolean dateFilter, ConfigHolder configHolder) {
    this.dateFilter = dateFilter;
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
        Cell cell = scanner.getCell();

        if (dateFilter) {
          byte[] tsBytes = new byte[TS_SIZE];
          System.arraycopy(cell.getRowArray(), cell.getRowOffset() + SKIP, tsBytes, 0, TS_SIZE);
          long baseMillis = Utilities.bytesToLong(tsBytes, 0, 4) * 1000L;

          if (baseMillis < MIN_DATE || baseMillis >= MAX_DATE) {
            return null;
          }
        }

        CellData value = new CellData(cell, file);

        hasNext = scanner.next();

        if (!hasNext) {
          reader.close();
        }

        return value;
      }
    };
  }
}
