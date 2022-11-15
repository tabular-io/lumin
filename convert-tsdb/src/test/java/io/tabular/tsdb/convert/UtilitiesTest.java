package io.tabular.tsdb.convert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class UtilitiesTest {

  private Random rnd = new Random();

  @Test
  public void testBytesToShort() {
    testBytesToShort((short) rnd.nextInt(1 << 16));
    testBytesToShort(Short.MAX_VALUE);
    testBytesToShort(Short.MIN_VALUE);
    testBytesToShort((short) 0);
  }

  private void testBytesToShort(short s) {
    ByteBuffer buf = ByteBuffer.allocate(2);
    buf.putShort(s);
    int result = Utilities.bytesToShort(buf.array(), 0, 2);
    assertEquals(s, result);
  }

  @Test
  public void testBytesToInt() {
    testBytesToInt(rnd.nextInt());
    testBytesToInt(Integer.MAX_VALUE);
    testBytesToInt(Integer.MIN_VALUE);
    testBytesToInt(0);
  }

  private void testBytesToInt(int i) {
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putInt(i);
    int result = Utilities.bytesToInt(buf.array(), 0, 4);
    assertEquals(i, result);
  }

  @Test
  public void testBytesToLong() {
    testBytesToLong(rnd.nextLong());
    testBytesToLong(Long.MAX_VALUE);
    testBytesToLong(Long.MIN_VALUE);
    testBytesToLong(0);
  }

  private void testBytesToLong(long l) {
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(l);
    long result = Utilities.bytesToLong(buf.array(), 0, 8);
    assertEquals(l, result);
  }

  @Test
  public void testBytesToFloat() {
    testBytesToFloat(rnd.nextFloat());
    testBytesToFloat(Float.MIN_VALUE);
    testBytesToFloat(Float.MAX_VALUE);
    testBytesToFloat(0);
  }

  private void testBytesToFloat(float f) {
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putFloat(f);
    int result = Utilities.bytesToInt(buf.array(), 0, 4);
    assertEquals(f, Float.intBitsToFloat(result));
  }

  @Test
  public void testBytesToDouble() {
    testBytesToDouble(rnd.nextDouble());
    testBytesToDouble(Double.MAX_VALUE);
    testBytesToDouble(Double.MIN_VALUE);
    testBytesToDouble(0);
  }

  private void testBytesToDouble(double d) {
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putDouble(d);
    long result = Utilities.bytesToLong(buf.array(), 0, 8);
    assertEquals(d, Double.longBitsToDouble(result));
  }
}
