package linkmesh.common;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flag parser supporting --key value, --key=value, bare --flag, and positionals.
 * Falls back to a LINKMESH_KEY environment variable when a flag is absent, so a
 * machine can be configured once and then started with no flags at all.
 */
public final class Args {
    private final Map<String, String> values = new LinkedHashMap<>();
    private final List<String> positional = new ArrayList<>();

    public Args(String[] argv) {
        for (int i = 0; i < argv.length; i++) {
            String token = argv[i];
            if (!token.startsWith("--")) { positional.add(token); continue; }
            String key = token.substring(2);
            String value;
            int eq = key.indexOf('=');
            if (eq >= 0) {
                value = key.substring(eq + 1);
                key = key.substring(0, eq);
            } else if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                value = argv[++i];
            } else {
                value = "true";
            }
            values.put(key, value);
        }
    }

    private String lookup(String key) {
        String value = values.get(key);
        if (value != null) return value;
        StringBuilder env = new StringBuilder("LINKMESH_");
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (Character.isUpperCase(c) && i > 0) env.append('_');
            env.append(Character.toUpperCase(c));
        }
        return System.getenv(env.toString());
    }

    public String get(String key, String fallback) {
        String value = lookup(key);
        return value == null ? fallback : value;
    }

    public String require(String key) {
        String value = lookup(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required flag --" + key);
        }
        return value;
    }

    public int getInt(String key, int fallback) {
        String value = lookup(key);
        return value == null ? fallback : Integer.parseInt(value.trim());
    }

    public long getLong(String key, long fallback) {
        String value = lookup(key);
        return value == null ? fallback : Long.parseLong(value.trim());
    }

    public double getDouble(String key, double fallback) {
        String value = lookup(key);
        return value == null ? fallback : Double.parseDouble(value.trim());
    }

    public boolean getBool(String key, boolean fallback) {
        String value = lookup(key);
        if (value == null) return fallback;
        return value.equalsIgnoreCase("true") || value.equals("1") || value.equalsIgnoreCase("yes");
    }

    public boolean has(String key) { return lookup(key) != null; }

    public List<String> positional() { return positional; }

    public String positional(int index, String fallback) {
        return index < positional.size() ? positional.get(index) : fallback;
    }
}
