import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * InputStream wrapper that ignores calls to close().
 */
class NonClosingInputStream extends FilterInputStream {
    protected NonClosingInputStream(InputStream in) {
        super(in);
    }

    @Override
    public void close() {
        // intentionally empty
    }

    @Override
    public synchronized void mark(int readlimit) {
        in.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        in.reset();
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }
}
