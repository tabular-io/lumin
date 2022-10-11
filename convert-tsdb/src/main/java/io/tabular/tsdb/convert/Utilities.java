package io.tabular.tsdb.convert;

public class Utilities {

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
}
