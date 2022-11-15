package io.tabular.tsdb.convert;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaSparkContext;

public class Utilities {

  private static final long GB_BYTES = 1_000_000_000;

  public static short bytesToShort(byte[] bytes, int offset, int len) {
    if (len > 2) {
      throw new RuntimeException("Invalid number of bytes for short: " + len);
    }

    // little-endian
    short result = 0;
    for (int i = offset; i < offset + len; i++) {
      result = (short) ((result << 8) | (bytes[i] & 0xff));
    }
    return result;
  }

  public static int bytesToInt(byte[] bytes) {
    return bytesToInt(bytes, 0, bytes.length);
  }

  public static int bytesToInt(byte[] bytes, int offset, int len) {
    if (len > 4) {
      throw new RuntimeException("Invalid number of bytes for int: " + len);
    }

    // little-endian
    int result = 0;
    for (int i = offset; i < offset + len; i++) {
      result = (result << 8) | (bytes[i] & 0xff);
    }
    return result;
  }

  public static long bytesToLong(byte[] bytes, int offset, int len) {
    if (len > 8) {
      throw new RuntimeException("Invalid number of bytes for long: " + len);
    }

    // little-endian
    long result = 0;
    for (int i = offset; i < offset + len; i++) {
      result = (result << 8) | (bytes[i] & 0xff);
    }
    return result;
  }

  public static List<String> sourceFiles(
      JavaSparkContext sparkContext, String sourceDir, int limitGb) {
    try {
      Path path = new Path(sourceDir);
      FileSystem fs = path.getFileSystem(sparkContext.hadoopConfiguration());

      List<FileStatus> fileList = Lists.newArrayList();
      long size = 0;
      RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, true);
      while (fileStatusListIterator.hasNext()) {
        LocatedFileStatus fileStatus = fileStatusListIterator.next();
        String name = fileStatus.getPath().getName();

        // this will filter out non-HFiles with names such as .tableinfo, .regioninfo, xxxx.yyyy
        if (fileStatus.isFile() && !name.contains(".") && !name.endsWith("_$folder$")) {
          fileList.add(fileStatus);
          size += fileStatus.getLen();
        }

        if (limitGb > 0 && size > limitGb * GB_BYTES) {
          break;
        }
      }

      // sort so we process the largest files first, to prevent long tail at the end of the job
      return fileList.stream()
          .sorted(Comparator.comparingLong(FileStatus::getLen).reversed())
          .map(fileStatus -> fileStatus.getPath().toString())
          .collect(Collectors.toList());
    } catch (IOException x) {
      throw new RuntimeException(x);
    }
  }
}
