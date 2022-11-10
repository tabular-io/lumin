package io.tabular.tsdb.convert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class UtilitiesTest {

  private Random rnd = new Random();

  @Test
  public void testBytesToInt() {
    int i = rnd.nextInt();
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putInt(i);
    int result = Utilities.bytesToInt(buf.array(), 0, 4);
    assertEquals(i, result);
  }

  @Test
  public void testBytesToLong() {
    long l = rnd.nextLong();
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(l);
    long result = Utilities.bytesToLong(buf.array(), 0, 8);
    assertEquals(l, result);
  }

  @Test
  public void testBytesToFloat() {
    float f = rnd.nextFloat();
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putFloat(f);
    int result = Utilities.bytesToInt(buf.array(), 0, 4);
    assertEquals(f, Float.intBitsToFloat(result));
  }

  @Test
  public void testBytesToDouble() {
    double d = rnd.nextDouble();
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putDouble(d);
    long result = Utilities.bytesToLong(buf.array(), 0, 8);
    assertEquals(d, Double.longBitsToDouble(result));
  }
}
