import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

class NonClosingInputStream extends FilterInputStream {
    protected NonClosingInputStream(InputStream in) {
        super(in);
    }

    public void close() throws IOException {
    }

    public synchronized void mark(int readlimit) {
        this.in.mark(readlimit);
    }

    public synchronized void reset() throws IOException {
        this.in.reset();
    }

    public boolean markSupported() {
        return this.in.markSupported();
    }
}
