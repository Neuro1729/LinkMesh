package linkmesh.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** Sidecar metadata for one stored partition replica. */
public record PartitionMeta(String id, long files, long bytes, String sha256, long storedAtMillis) {

    public static PartitionMeta read(Path metaFile) throws IOException {
        Map<String, String> values = new HashMap<>();
        for (String line : Files.readAllLines(metaFile, StandardCharsets.UTF_8)) {
            int eq = line.indexOf('=');
            if (eq > 0) values.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
        }
        return new PartitionMeta(
                values.getOrDefault("id", metaFile.getFileName().toString().replace(".meta", "")),
                Long.parseLong(values.getOrDefault("files", "0")),
                Long.parseLong(values.getOrDefault("bytes", "0")),
                values.getOrDefault("sha256", ""),
                Long.parseLong(values.getOrDefault("storedAtMillis", "0")));
    }

    public void write(Path metaFile) throws IOException {
        String content = "id=" + id + "\n"
                + "files=" + files + "\n"
                + "bytes=" + bytes + "\n"
                + "sha256=" + sha256 + "\n"
                + "storedAtMillis=" + storedAtMillis + "\n";
        Files.writeString(metaFile, content, StandardCharsets.UTF_8);
    }
}
