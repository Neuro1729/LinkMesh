package linkmesh.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Hashing {
    private Hashing() {}

    public static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    public static String sha256Hex(byte[] input) {
        return hex(sha256().digest(input));
    }

    public static String sha256Hex(InputStream in) throws IOException {
        MessageDigest digest = sha256();
        byte[] buffer = new byte[64 * 1024];
        int read;
        while ((read = in.read(buffer)) > 0) digest.update(buffer, 0, read);
        return hex(digest.digest());
    }

    /**
     * Stable 32-bit hash used for all partitioning decisions. Defined explicitly
     * rather than relying on String.hashCode() so the mapping from key to
     * partition can never shift if the key type or JDK changes.
     */
    public static int partitionHash(String key) {
        int h = 0;
        for (byte b : key.getBytes(StandardCharsets.UTF_8)) h = 31 * h + (b & 0xFF);
        return h;
    }

    public static int bucket(String key, int buckets) {
        return Math.floorMod(partitionHash(key), buckets);
    }
}
