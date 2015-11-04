package info.benjaminhill.util;

import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferInt;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Hash various stuff to a readable string.
 *
 * @author benjamin
 */
public class HashUtil {

  private static final String ENCODE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";
  private static final HashFunction HF = Hashing.murmur3_128();
  private static final CharMatcher CM_1 = CharMatcher.is('1');

  private static byte[] hash(final int[] input) {
    final ByteBuffer bb = ByteBuffer.allocate(input.length * 4);
    bb.asIntBuffer().put(input);
    return HF.hashBytes(bb.array()).asBytes();
  }

  public static byte[] hash(final String input) {
    return HF.hashBytes(input.getBytes(Charsets.UTF_8)).asBytes();
  }

  /**
   * Normally not needed image to integer pixel data
   *
   * @param bi
   * @return
   */
  public static byte[] hash(final BufferedImage bi) {
    BufferedImage tmp = bi;
    if (bi.getType() != BufferedImage.TYPE_INT_RGB) {
      tmp = new BufferedImage(bi.getWidth(), bi.getHeight(), BufferedImage.TYPE_INT_RGB);
      tmp.getGraphics().drawImage(bi, 0, 0, null);
    }
    final DataBufferInt db1 = (DataBufferInt) tmp.getRaster().getDataBuffer();
    Preconditions.checkState(db1.getNumBanks() == 1);
    return hash(db1.getData());
  }

  /**
   *
   * @param l1
   * @param l2
   * @return
   */
  public static int hammingDistance(final byte[] l1, final byte[] l2) {
    Preconditions.checkNotNull(l1);
    Preconditions.checkNotNull(l2);
    return CM_1.countIn(Long.toBinaryString(ByteBuffer.wrap(l1).getLong() ^ ByteBuffer.wrap(l2).getLong()));
  }

  public static int hammingDistance(final String s1, final String s2) {
    Preconditions.checkNotNull(s1);
    Preconditions.checkNotNull(s2);
    int counter = 0;
    for (int k = 0; k < s1.length(); k++) {
      if (s1.charAt(k) != s2.charAt(k)) {
        counter++;
      }
    }
    return counter;
  }

  /**
   *
   * @param f
   * @return Now using Guava hasher, better than before!
   */
  public static byte[] hash(final File f) {
    try {
      return Files.hash(f, HF).asBytes();
    } catch (final IOException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  public static byte[] hash(final byte[] input) {
    return HF.hashBytes(input).asBytes();
  }

  /**
   * Represent bytes (from a hash) as a nice printable string
   *
   * @param mdbytes
   * @return
   */
  public static String encodeToString(final byte[] mdbytes) {
    return encodeToString(new BigInteger(1, mdbytes));
  }

  /**
   * Big integer to much smaller string encoding
   *
   * @param input
   * @return
   */
  private static String encodeToString(final BigInteger input) {
    final BigInteger encodeLength = BigInteger.valueOf(ENCODE.length());
    BigInteger running = input;
    BigInteger mod;
    final StringBuilder result = new StringBuilder(64);
    result.append(running.intValue() == 0 ? "0" : "");
    while (running.longValue() != 0) {
      mod = running.remainder(encodeLength);
      result.append(ENCODE.substring(mod.intValue(), mod.intValue() + 1));
      running = running.divide(encodeLength);
    }
    return result.reverse().toString();
  }

  private HashUtil() {
    // empty, util class
  }

}