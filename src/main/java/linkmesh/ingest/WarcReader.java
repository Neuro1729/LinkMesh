package linkmesh.ingest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Streaming reader for WARC and WARC.GZ files.
 *
 * The format is simple enough to parse directly, which keeps the project free of
 * third-party dependencies:
 *
 *   WARC/1.0 CRLF
 *   Header-Name: value CRLF      (repeated)
 *   CRLF
 *   &lt;Content-Length bytes of body&gt;
 *   CRLF CRLF
 *
 * A .warc.gz is a concatenation of independently gzipped records rather than one
 * gzip stream. GZIPInputStream reads concatenated members transparently, so the
 * whole file can be wrapped once and read start to finish.
 *
 * Records are streamed one at a time and oversized bodies are skipped rather
 * than buffered, so memory stays flat regardless of input size.
 */
public final class WarcReader implements Closeable {
    private static final int MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;

    private final InputStream in;
    private long recordsRead;
    private long recordsSkipped;

    public WarcReader(InputStream raw) {
        this.in = new BufferedInputStream(raw, 256 * 1024);
    }

    public static WarcReader open(Path file) throws IOException {
        InputStream stream = new BufferedInputStream(Files.newInputStream(file), 256 * 1024);
        String name = file.getFileName().toString().toLowerCase();
        if (name.endsWith(".gz")) stream = new GZIPInputStream(stream, 128 * 1024);
        return new WarcReader(stream);
    }

    public long recordsRead() { return recordsRead; }

    public long recordsSkipped() { return recordsSkipped; }

    /** Returns the next record, or null at end of file. */
    public WarcRecord next() throws IOException {
        String line;
        // Skip any inter-record padding until a version line shows up.
        while ((line = readLine()) != null && !line.startsWith("WARC/")) {
            // intentionally empty
        }
        if (line == null) return null;

        Map<String, String> warcHeaders = readHeaders();
        int contentLength = parseInt(warcHeaders.get("content-length"), -1);
        if (contentLength < 0) return null;

        byte[] body;
        if (contentLength > MAX_PAYLOAD_BYTES) {
            skipFully(contentLength);
            body = new byte[0];
            recordsSkipped++;
        } else {
            body = new byte[contentLength];
            readFully(body);
        }
        consumeTrailer();
        recordsRead++;

        // For a response record the body is itself an HTTP message, so the real
        // page content sits after another header block.
        Map<String, String> httpHeaders = new HashMap<>();
        byte[] payload = body;
        if ("response".equals(warcHeaders.getOrDefault("warc-type", "")) && body.length > 0) {
            int split = findHeaderEnd(body);
            if (split > 0) {
                httpHeaders = parseHttpHeaders(new String(body, 0, split, StandardCharsets.ISO_8859_1));
                payload = new byte[body.length - split];
                System.arraycopy(body, split, payload, 0, payload.length);
            }
        }
        return new WarcRecord(warcHeaders, httpHeaders, payload);
    }

    private Map<String, String> readHeaders() throws IOException {
        Map<String, String> headers = new LinkedHashMap<>();
        String line;
        while ((line = readLine()) != null && !line.isEmpty()) {
            int colon = line.indexOf(':');
            if (colon <= 0) continue;
            headers.put(line.substring(0, colon).trim().toLowerCase(), line.substring(colon + 1).trim());
        }
        return headers;
    }

    private static Map<String, String> parseHttpHeaders(String block) {
        Map<String, String> headers = new HashMap<>();
        for (String line : block.split("\r\n|\n")) {
            int colon = line.indexOf(':');
            if (colon <= 0) continue;
            headers.put(line.substring(0, colon).trim().toLowerCase(), line.substring(colon + 1).trim());
        }
        return headers;
    }

    /** Index just past the blank line that ends the HTTP header block. */
    private static int findHeaderEnd(byte[] body) {
        for (int i = 0; i + 3 < body.length; i++) {
            if (body[i] == '\r' && body[i + 1] == '\n' && body[i + 2] == '\r' && body[i + 3] == '\n') return i + 4;
        }
        for (int i = 0; i + 1 < body.length; i++) {
            if (body[i] == '\n' && body[i + 1] == '\n') return i + 2;
        }
        return -1;
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(128);
        int b;
        while ((b = in.read()) >= 0) {
            if (b == '\n') {
                int size = buffer.size();
                byte[] bytes = buffer.toByteArray();
                if (size > 0 && bytes[size - 1] == '\r') size--;
                return new String(bytes, 0, size, StandardCharsets.ISO_8859_1);
            }
            buffer.write(b);
        }
        return buffer.size() == 0 ? null : buffer.toString(StandardCharsets.ISO_8859_1);
    }

    private void readFully(byte[] target) throws IOException {
        int offset = 0;
        while (offset < target.length) {
            int read = in.read(target, offset, target.length - offset);
            if (read < 0) throw new EOFException("truncated WARC record");
            offset += read;
        }
    }

    private void skipFully(long count) throws IOException {
        long remaining = count;
        byte[] scratch = new byte[64 * 1024];
        while (remaining > 0) {
            int read = in.read(scratch, 0, (int) Math.min(scratch.length, remaining));
            if (read < 0) throw new EOFException("truncated WARC record");
            remaining -= read;
        }
    }

    private void consumeTrailer() throws IOException {
        in.mark(4);
        for (int i = 0; i < 2; i++) {
            int c = in.read();
            if (c == '\r') c = in.read();
            if (c != '\n') { in.reset(); return; }
        }
    }

    private static int parseInt(String value, int fallback) {
        try {
            return value == null ? fallback : Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
