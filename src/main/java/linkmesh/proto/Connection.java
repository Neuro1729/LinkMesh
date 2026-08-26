package linkmesh.proto;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A framed message channel over one TCP socket, reused across many messages.
 *
 * Not safe for concurrent senders. Callers either own the connection (borrowed
 * from the pool) or synchronize externally.
 */
public final class Connection implements Closeable {
    private static final int MAX_HEADER_BYTES = 64 * 1024;
    private static final long MAX_BODY_BYTES = 512L * 1024 * 1024;

    private final Socket socket;
    private final Endpoint remote;
    private final BufferedInputStream in;
    private final BufferedOutputStream out;
    private final AtomicLong lastUsedNanos = new AtomicLong(System.nanoTime());
    private final Object writeLock = new Object();

    private volatile Object attachment;
    private volatile boolean broken;

    private Connection(Socket socket, Endpoint remote) throws IOException {
        this.socket = socket;
        this.remote = remote;
        this.in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);
        this.out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
    }

    public static Connection connect(Endpoint endpoint, int connectTimeoutMs, int readTimeoutMs) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(endpoint.toSocketAddress(), connectTimeoutMs);
            socket.setSoTimeout(readTimeoutMs);
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            return new Connection(socket, endpoint);
        } catch (IOException e) {
            try { socket.close(); } catch (IOException ignored) { }
            throw e;
        }
    }

    public static Connection accept(Socket socket, int readTimeoutMs) throws IOException {
        socket.setSoTimeout(readTimeoutMs);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        Endpoint remote = new Endpoint(socket.getInetAddress().getHostAddress(), socket.getPort());
        return new Connection(socket, remote);
    }

    public Endpoint remote() { return remote; }

    public Object attachment() { return attachment; }

    public void attach(Object value) { this.attachment = value; }

    public boolean isUsable() {
        return !broken && !socket.isClosed() && socket.isConnected();
    }

    public long idleNanos() { return System.nanoTime() - lastUsedNanos.get(); }

    public void send(Message message) throws IOException {
        byte[] header = message.encodeHeader().getBytes(StandardCharsets.UTF_8);
        byte[] body = message.body();
        synchronized (writeLock) {
            try {
                out.write(header);
                out.write('\n');
                if (body.length > 0) out.write(body);
                out.flush();
            } catch (IOException e) {
                broken = true;
                throw e;
            }
        }
        lastUsedNanos.set(System.nanoTime());
    }

    /** Reads the next message, or null on a clean end-of-stream. */
    public Message receive() throws IOException {
        String headerLine;
        try {
            headerLine = readLine();
        } catch (SocketException e) {
            broken = true;
            throw e;
        }
        if (headerLine == null) return null;
        if (headerLine.isBlank()) throw new ProtocolException("blank header line");

        Message header = Message.decodeHeader(headerLine);
        long length = header.getLong(Message.LEN, 0);
        if (length < 0 || length > MAX_BODY_BYTES) {
            broken = true;
            throw new ProtocolException("illegal body length: " + length);
        }
        lastUsedNanos.set(System.nanoTime());
        if (length == 0) return header;

        byte[] body = new byte[(int) length];
        int offset = 0;
        while (offset < body.length) {
            int read = in.read(body, offset, body.length - offset);
            if (read < 0) {
                broken = true;
                throw new EOFException("truncated body: got " + offset + " of " + body.length);
            }
            offset += read;
        }
        lastUsedNanos.set(System.nanoTime());
        return header.withBody(body);
    }

    /** Sends a request and blocks for the matching reply. */
    public Message request(Message message) throws IOException {
        send(message);
        Message reply = receive();
        if (reply == null) {
            broken = true;
            throw new EOFException("peer closed while awaiting reply to " + message.verb());
        }
        return reply;
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(256);
        int b;
        while ((b = in.read()) >= 0) {
            if (b == '\n') return buffer.toString(StandardCharsets.UTF_8);
            if (buffer.size() >= MAX_HEADER_BYTES) {
                broken = true;
                throw new ProtocolException("header line exceeded " + MAX_HEADER_BYTES + " bytes");
            }
            buffer.write(b);
        }
        return buffer.size() == 0 ? null : buffer.toString(StandardCharsets.UTF_8);
    }

    public void setReadTimeout(int millis) {
        try { socket.setSoTimeout(millis); } catch (SocketException ignored) { }
    }

    public void markBroken() { broken = true; }

    @Override
    public void close() {
        broken = true;
        try { socket.close(); } catch (IOException ignored) { }
    }

    @Override
    public String toString() { return "conn->" + remote; }
}
