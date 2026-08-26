package linkmesh.common;

/**
 * Escaping for protocol header values. Keeps the wire format human-readable --
 * URLs stay legible in logs and tcpdump -- while making tabs and newlines safe.
 */
public final class Text {
    private Text() {}

    public static String escape(String value) {
        if (value == null) return "";
        StringBuilder sb = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '\t' -> sb.append("\\t");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    public static String unescape(String value) {
        if (value == null) return "";
        if (value.indexOf('\\') < 0) return value;
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != '\\' || i + 1 >= value.length()) { sb.append(c); continue; }
            char next = value.charAt(++i);
            switch (next) {
                case '\\' -> sb.append('\\');
                case 't' -> sb.append('\t');
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                default -> sb.append('\\').append(next);
            }
        }
        return sb.toString();
    }

    public static String humanBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024L * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    public static String humanMillis(long millis) {
        if (millis < 1000) return millis + " ms";
        if (millis < 60_000) return String.format("%.1f s", millis / 1000.0);
        return String.format("%d m %d s", millis / 60_000, (millis % 60_000) / 1000);
    }
}
