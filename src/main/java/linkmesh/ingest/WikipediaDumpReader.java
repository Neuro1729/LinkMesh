package linkmesh.ingest;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

/**
 * Streams articles out of a Wikipedia Enterprise HTML dump.
 *
 * The file is a gzipped tar of NDJSON members, one JSON object per line holding
 * the article URL and its rendered HTML. Both layers stream, so a multi-GB dump
 * never lands on disk unpacked and only one record is held at a time.
 *
 * Truncated input counts as end of file. A partial download is a fine corpus,
 * and stopping once you have enough articles beats fetching all 2 GB.
 */
public final class WikipediaDumpReader implements Closeable {
    private static final int TAR_BLOCK = 512;
    private static final int MAX_LINE_BYTES = 32 * 1024 * 1024;

    /** One article: its canonical URL and the rendered HTML body. */
    public record Article(String url, String html) {}

    private final InputStream in;

    private long remainingInMember;
    private long currentMemberSize;
    private boolean exhausted;
    private byte[] carry = new byte[0];
    private long recordsRead;
    private long recordsSkipped;

    public WikipediaDumpReader(InputStream raw) {
        this.in = new BufferedInputStream(raw, 1 << 20);
    }

    public static WikipediaDumpReader open(Path file) throws IOException {
        InputStream stream = new BufferedInputStream(Files.newInputStream(file), 1 << 20);
        String name = file.getFileName().toString().toLowerCase();
        if (name.endsWith(".gz") || name.endsWith(".tgz")) {
            stream = new GZIPInputStream(stream, 1 << 20);
        }
        return new WikipediaDumpReader(stream);
    }

    public long recordsRead() { return recordsRead; }

    public long recordsSkipped() { return recordsSkipped; }

    /** Returns the next article, or null once the dump is exhausted. */
    public Article next() throws IOException {
        while (!exhausted) {
            byte[] line = nextLine();
            if (line == null) return null;
            if (line.length < 2) continue;

            recordsRead++;
            String json = new String(line, StandardCharsets.UTF_8);
            String url = Json.getString(json, "url");
            String html = Json.getString(json, "article_body.html");
            if (url == null || html == null || html.isEmpty()) {
                recordsSkipped++;
                continue;
            }
            return new Article(url, html);
        }
        return null;
    }

    /** Reassembles NDJSON lines across tar block and read boundaries. */
    private byte[] nextLine() throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream(64 * 1024);
        while (true) {
            int newline = indexOf(carry, (byte) '\n');
            if (newline >= 0) {
                line.write(carry, 0, newline);
                byte[] rest = new byte[carry.length - newline - 1];
                System.arraycopy(carry, newline + 1, rest, 0, rest.length);
                carry = rest;
                return line.toByteArray();
            }
            line.write(carry, 0, carry.length);
            carry = new byte[0];
            if (line.size() > MAX_LINE_BYTES) throw new IOException("NDJSON line exceeded size limit");

            byte[] chunk = readMemberChunk();
            if (chunk == null) {
                exhausted = true;
                return line.size() > 0 ? line.toByteArray() : null;
            }
            carry = chunk;
        }
    }

    /** Reads the next slice of the current tar member, advancing members as needed. */
    private byte[] readMemberChunk() throws IOException {
        try {
            while (remainingInMember == 0) {
                if (!advanceToNextMember()) return null;
            }
            int want = (int) Math.min(1 << 20, remainingInMember);
            byte[] buffer = new byte[want];
            int read = readAtMost(buffer);
            if (read <= 0) return null;
            remainingInMember -= read;
            if (remainingInMember == 0) skipMemberPadding();
            if (read == buffer.length) return buffer;
            byte[] exact = new byte[read];
            System.arraycopy(buffer, 0, exact, 0, read);
            return exact;
        } catch (EOFException e) {
            // A truncated archive is expected when only part of the dump was
            // fetched. Everything decoded so far is still valid.
            return null;
        }
    }

    private boolean advanceToNextMember() throws IOException {
        byte[] header = new byte[TAR_BLOCK];
        int read = readAtMost(header);
        if (read < TAR_BLOCK) return false;

        boolean allZero = true;
        for (byte b : header) {
            if (b != 0) { allZero = false; break; }
        }
        if (allZero) return false;

        long size = parseOctal(header, 124, 12);
        char type = (char) header[156];
        if (type != '0' && type != 0 && type != '7') {
            // Directory or metadata entry: no payload, keep looking.
            skipExactly(roundUp(size));
            return true;
        }
        remainingInMember = size;
        currentMemberSize = size;
        if (size == 0) skipMemberPadding();
        return true;
    }

    /** Tar pads every member out to a 512-byte boundary; step over the filler. */
    private void skipMemberPadding() throws IOException {
        skipExactly(roundUp(currentMemberSize) - currentMemberSize);
        currentMemberSize = 0;
    }

    private static long roundUp(long size) {
        long remainder = size % TAR_BLOCK;
        return remainder == 0 ? size : size + (TAR_BLOCK - remainder);
    }

    private static long parseOctal(byte[] block, int offset, int length) {
        long value = 0;
        for (int i = offset; i < offset + length; i++) {
            int c = block[i] & 0xFF;
            if (c == 0 || c == ' ') continue;
            if (c < '0' || c > '7') continue;
            value = value * 8 + (c - '0');
        }
        return value;
    }

    private int readAtMost(byte[] buffer) throws IOException {
        int offset = 0;
        while (offset < buffer.length) {
            int read = in.read(buffer, offset, buffer.length - offset);
            if (read < 0) break;
            offset += read;
        }
        return offset;
    }

    private void skipExactly(long count) throws IOException {
        long remaining = count;
        byte[] scratch = new byte[8192];
        while (remaining > 0) {
            int read = in.read(scratch, 0, (int) Math.min(scratch.length, remaining));
            if (read < 0) return;
            remaining -= read;
        }
    }

    private static int indexOf(byte[] data, byte target) {
        for (int i = 0; i < data.length; i++) {
            if (data[i] == target) return i;
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
