package linkmesh.proto;

import linkmesh.common.Hashing;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming archive for moving a partition directory between nodes:
 *
 *   "LMAR" magic, int version, then repeating
 *     int nameLength, UTF-8 relative name, long size, size bytes
 *   terminated by nameLength == -1
 */
public final class Archive {
    private static final byte[] MAGIC = {'L', 'M', 'A', 'R'};
    private static final int VERSION = 1;
    private static final int END = -1;

    private Archive() {}

    public record Written(long bytes, long files, String sha256) {}

    /** Packs every regular file under root, returning the digest of the archive stream. */
    public static Written pack(Path root, OutputStream sink) throws IOException {
        DigestOutputStream digest = new DigestOutputStream(sink, Hashing.sha256());
        CountingOutputStream counter = new CountingOutputStream(digest);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(counter, 64 * 1024));

        out.write(MAGIC);
        out.writeInt(VERSION);

        List<Path> files = listFiles(root);
        for (Path file : files) {
            String name = root.relativize(file).toString().replace('\\', '/');
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            long size = Files.size(file);
            out.writeInt(nameBytes.length);
            out.write(nameBytes);
            out.writeLong(size);
            try (InputStream fileIn = new BufferedInputStream(Files.newInputStream(file), 64 * 1024)) {
                fileIn.transferTo(out);
            }
        }
        out.writeInt(END);
        out.flush();
        return new Written(counter.count(), files.size(), Hashing.hex(digest.getMessageDigest().digest()));
    }

    /** Unpacks into target, rejecting any entry that would escape the directory. */
    public static Written unpack(InputStream source, Path target) throws IOException {
        Files.createDirectories(target);
        Path targetRoot = target.toRealPath();
        DigestOutputStream digestSink = new DigestOutputStream(OutputStream.nullOutputStream(), Hashing.sha256());
        DataInputStream in = new DataInputStream(new BufferedInputStream(new DigestingInputStream(source, digestSink), 64 * 1024));

        byte[] magic = new byte[4];
        in.readFully(magic);
        if (magic[0] != MAGIC[0] || magic[1] != MAGIC[1] || magic[2] != MAGIC[2] || magic[3] != MAGIC[3]) {
            throw new ProtocolException("not a linkmesh archive");
        }
        int version = in.readInt();
        if (version != VERSION) throw new ProtocolException("unsupported archive version " + version);

        long bytes = 0;
        long files = 0;
        while (true) {
            int nameLength = in.readInt();
            if (nameLength == END) break;
            if (nameLength < 0 || nameLength > 4096) throw new ProtocolException("bad entry name length " + nameLength);
            byte[] nameBytes = new byte[nameLength];
            in.readFully(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);
            long size = in.readLong();
            if (size < 0) throw new ProtocolException("bad entry size " + size);

            Path destination = target.resolve(name).normalize();
            if (!destination.startsWith(targetRoot) && !destination.startsWith(target)) {
                throw new ProtocolException("archive entry escapes target: " + name);
            }
            Files.createDirectories(destination.getParent());
            try (OutputStream fileOut = new BufferedOutputStream(Files.newOutputStream(destination), 64 * 1024)) {
                copyExactly(in, fileOut, size);
            }
            bytes += size;
            files++;
        }
        return new Written(bytes, files, Hashing.hex(digestSink.getMessageDigest().digest()));
    }

    private static void copyExactly(InputStream in, OutputStream out, long count) throws IOException {
        byte[] buffer = new byte[64 * 1024];
        long remaining = count;
        while (remaining > 0) {
            int read = in.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (read < 0) throw new EOFException("archive truncated, " + remaining + " bytes short");
            out.write(buffer, 0, read);
            remaining -= read;
        }
    }

    private static List<Path> listFiles(Path root) throws IOException {
        List<Path> files = new ArrayList<>();
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (attrs.isRegularFile()) files.add(file);
                return FileVisitResult.CONTINUE;
            }
        });
        files.sort(Path::compareTo);
        return files;
    }

    private static final class CountingOutputStream extends FilterOutputStream {
        private long count;

        CountingOutputStream(OutputStream out) { super(out); }

        @Override public void write(int b) throws IOException { out.write(b); count++; }

        @Override public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            count += len;
        }

        long count() { return count; }
    }

    /** Feeds every byte read into a digest so the receiver can verify integrity. */
    private static final class DigestingInputStream extends FilterInputStream {
        private final OutputStream digest;

        DigestingInputStream(InputStream in, OutputStream digest) { super(in); this.digest = digest; }

        @Override public int read() throws IOException {
            int b = in.read();
            if (b >= 0) digest.write(b);
            return b;
        }

        @Override public int read(byte[] b, int off, int len) throws IOException {
            int read = in.read(b, off, len);
            if (read > 0) digest.write(b, off, read);
            return read;
        }
    }
}
