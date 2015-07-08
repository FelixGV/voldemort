package voldemort.store.readonly.fetcher;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

/**
 * Simple class to wrap some of the inner function of {@link GZIPInputStream} and
 * {@link java.util.zip.InflaterInputStream} for better instrumentation.
 */
public class InstrumentedGzipInputStream extends GZIPInputStream {
    AtomicInteger innerStreamBytesRead = new AtomicInteger(0);

    /**
     * Creates a new input stream with the specified buffer size.
     *
     * @param in   the input stream
     * @param size the input buffer size
     * @throws java.io.IOException if an I/O error has occurred
     * @throws IllegalArgumentException
     *                             if size is <= 0
     */
    public InstrumentedGzipInputStream(InputStream in, int size) throws IOException {
        super(in, size);
    }

    @Override
    protected void fill() throws IOException {
        super.fill();
        innerStreamBytesRead.addAndGet(len);
    }

    public int getAndResetBytesReadFromInnerStream() {
        int read = innerStreamBytesRead.getAndSet(0);
        return read;
    }
}