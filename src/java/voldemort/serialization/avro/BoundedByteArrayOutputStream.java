package voldemort.serialization.avro;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * A tweaked version of {@link java.io.ByteArrayOutputStream} which has a
 * bounded length on its internal buffer.
 *
 * This class implements an output stream in which the data is
 * written into a byte array. The buffer automatically grows as data
 * is written to it.
 * The data can be retrieved using <code>toByteArray()</code> and
 * <code>toString()</code>.
 * <p>
 * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods in
 * this class can be called after the stream has been closed without
 * generating an <tt>IOException</tt>.
 *
 * @author  Arthur van Hoff
 * @since   JDK1.0
 */
public class BoundedByteArrayOutputStream extends OutputStream {
  /**
   * The buffer where data is stored.
   */
  protected byte buf[];

  /**
   * The maximum size of the buffer.
   */
  protected int maxSize;

  /**
   * The number of valid bytes in the buffer.
   */
  protected int count;

  /**
   * Creates a new byte array output stream. The buffer capacity is
   * initially 32 bytes, though its size increases if necessary.
   */
  public BoundedByteArrayOutputStream() {
    this(32);
  }

  /**
   * Creates a new byte array output stream, with a buffer capacity of
   * the specified size, in bytes.
   *
   * @param   initialSize   the initial size.
   * @exception  IllegalArgumentException if size is negative.
   */
  public BoundedByteArrayOutputStream(int initialSize) {
    this(initialSize, Integer.MAX_VALUE);
  }

  /**
   * Creates a new byte array output stream, with a specified initial
   * and max capacity, in bytes.
   *
   * @param   initialSize   the initial size.
   * @param   maxSize   the max size.
   * @exception  IllegalArgumentException if size is negative.
   */
  public BoundedByteArrayOutputStream(int initialSize, int maxSize) {
    if (initialSize < 0) {
      throw new IllegalArgumentException("Negative initial size: "
          + initialSize);
    }
    if (maxSize < 1) {
      throw new IllegalArgumentException("Max size less than 1: "
          + maxSize);
    }
    buf = new byte[initialSize];
    this.maxSize = maxSize;
  }

  /**
   * Increases the capacity if necessary to ensure that it can hold
   * at least the number of elements specified by the minimum
   * capacity argument.
   *
   * @param minCapacity the desired minimum capacity
   * @throws OutOfMemoryError if {@code minCapacity < 0}.  This is
   * interpreted as a request for the unsatisfiably large capacity
   * {@code (long) Integer.MAX_VALUE + (minCapacity - Integer.MAX_VALUE)}.
   */
  private void ensureCapacity(int minCapacity) {
    // overflow-conscious code
    if (minCapacity - buf.length > 0)
      grow(minCapacity);
  }

  /**
   * Increases the capacity to ensure that it can hold at least the
   * number of elements specified by the minimum capacity argument.
   *
   * @param minCapacity the desired minimum capacity
   */
  private void grow(int minCapacity) {
    // overflow-conscious code
    int oldCapacity = buf.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity - minCapacity < 0)
      newCapacity = minCapacity;
    if (newCapacity < 0 || newCapacity > maxSize) {
      if (minCapacity < 0) // overflow
        throw new OutOfMemoryError();
      newCapacity = maxSize;
    }
    buf = Arrays.copyOf(buf, newCapacity);
  }

  /**
   * Writes the specified byte to this byte array output stream.
   *
   * @param   b   the byte to be written.
   */
  public synchronized void write(int b) {
    ensureCapacity(count + 1);
    buf[count] = (byte) b;
    count += 1;
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array
   * starting at offset <code>off</code> to this byte array output stream.
   *
   * @param   b     the data.
   * @param   off   the start offset in the data.
   * @param   len   the number of bytes to write.
   */
  public synchronized void write(byte b[], int off, int len) {
    if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) - b.length > 0)) {
      throw new IndexOutOfBoundsException();
    }
    ensureCapacity(count + len);
    try {
      System.arraycopy(b, off, buf, count, len);
    } catch (IndexOutOfBoundsException e) {
      throw new IndexOutOfBoundsException("Current buffer size: " + buf.length +
          ", max buffer size: " + maxSize +
          ", current byte count: " + count +
          ", attempting to write " + len + " additional bytes.");
    }
    count += len;
  }

  /**
   * Writes the complete contents of this byte array output stream to
   * the specified output stream argument, as if by calling the output
   * stream's write method using <code>out.write(buf, 0, count)</code>.
   *
   * @param      out   the output stream to which to write the data.
   * @exception IOException  if an I/O error occurs.
   */
  public synchronized void writeTo(OutputStream out) throws IOException {
    out.write(buf, 0, count);
  }

  /**
   * Resets the <code>count</code> field of this byte array output
   * stream to zero, so that all currently accumulated output in the
   * output stream is discarded. The output stream can be used again,
   * reusing the already allocated buffer space.
   *
   * @see     java.io.ByteArrayInputStream#count
   */
  public synchronized void reset() {
    count = 0;
  }

  /**
   * Creates a newly allocated byte array. Its size is the current
   * size of this output stream and the valid contents of the buffer
   * have been copied into it.
   *
   * @return  the current contents of this output stream, as a byte array.
   * @see     java.io.ByteArrayOutputStream#size()
   */
  public synchronized byte toByteArray()[] {
    return Arrays.copyOf(buf, count);
  }

  /**
   * Returns the current size of the buffer.
   *
   * @return  the value of the <code>count</code> field, which is the number
   *          of valid bytes in this output stream.
   * @see     java.io.ByteArrayOutputStream#count
   */
  public synchronized int size() {
    return count;
  }

  /**
   * Converts the buffer's contents into a string decoding bytes using the
   * platform's default character set. The length of the new <tt>String</tt>
   * is a function of the character set, and hence may not be equal to the
   * size of the buffer.
   *
   * <p> This method always replaces malformed-input and unmappable-character
   * sequences with the default replacement string for the platform's
   * default character set. The {@linkplain java.nio.charset.CharsetDecoder}
   * class should be used when more control over the decoding process is
   * required.
   *
   * @return String decoded from the buffer's contents.
   * @since  JDK1.1
   */
  public synchronized String toString() {
    return new String(buf, 0, count);
  }

  /**
   * Converts the buffer's contents into a string by decoding the bytes using
   * the named {@link java.nio.charset.Charset charset}. The length of the new
   * <tt>String</tt> is a function of the charset, and hence may not be equal
   * to the length of the byte array.
   *
   * <p> This method always replaces malformed-input and unmappable-character
   * sequences with this charset's default replacement string. The {@link
   * java.nio.charset.CharsetDecoder} class should be used when more control
   * over the decoding process is required.
   *
   * @param      charsetName  the name of a supported
   *             {@link java.nio.charset.Charset charset}
   * @return     String decoded from the buffer's contents.
   * @exception UnsupportedEncodingException
   *             If the named charset is not supported
   * @since      JDK1.1
   */
  public synchronized String toString(String charsetName)
      throws UnsupportedEncodingException
  {
    return new String(buf, 0, count, charsetName);
  }

  /**
   * Creates a newly allocated string. Its size is the current size of
   * the output stream and the valid contents of the buffer have been
   * copied into it. Each character <i>c</i> in the resulting string is
   * constructed from the corresponding element <i>b</i> in the byte
   * array such that:
   * <blockquote><pre>
   *     c == (char)(((hibyte &amp; 0xff) &lt;&lt; 8) | (b &amp; 0xff))
   * </pre></blockquote>
   *
   * @deprecated This method does not properly convert bytes into characters.
   * As of JDK&nbsp;1.1, the preferred way to do this is via the
   * <code>toString(String enc)</code> method, which takes an encoding-name
   * argument, or the <code>toString()</code> method, which uses the
   * platform's default character encoding.
   *
   * @param      hibyte    the high byte of each resulting Unicode character.
   * @return     the current contents of the output stream, as a string.
   * @see        java.io.ByteArrayOutputStream#size()
   * @see        java.io.ByteArrayOutputStream#toString(String)
   * @see        java.io.ByteArrayOutputStream#toString()
   */
  @Deprecated
  public synchronized String toString(int hibyte) {
    return new String(buf, hibyte, 0, count);
  }

  /**
   * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods in
   * this class can be called after the stream has been closed without
   * generating an <tt>IOException</tt>.
   */
  public void close() throws IOException {
  }

}

