package linkmesh.ingest;

/**
 * Just enough JSON to pull named fields out of one object.
 *
 * Dump records run ~40 KB each and we want two fields, so this walks the text
 * and skips values it was not asked for rather than building the whole document.
 *
 * A substring search would be shorter and wrong: article HTML contains text that
 * looks like JSON keys, and only a real parse tells a key from the same bytes
 * sitting inside a string value.
 */
public final class Json {

    private final String text;
    private int pos;

    private Json(String text) {
        this.text = text;
    }

    /**
     * Reads {@code object[name]} as a string, where name may be a dotted path
     * such as {@code article_body.html}. Returns null if absent or not a string.
     */
    public static String getString(String json, String path) {
        Json reader = new Json(json);
        reader.skipWhitespace();
        String[] segments = path.split("\\.");
        return reader.findString(segments, 0);
    }

    private String findString(String[] segments, int depth) {
        if (!expect('{')) return null;
        skipWhitespace();
        if (peek() == '}') { pos++; return null; }

        while (pos < text.length()) {
            skipWhitespace();
            String key = readString();
            if (key == null) return null;
            skipWhitespace();
            if (!expect(':')) return null;
            skipWhitespace();

            if (key.equals(segments[depth])) {
                if (depth == segments.length - 1) {
                    return peek() == '"' ? readString() : null;
                }
                return peek() == '{' ? findString(segments, depth + 1) : null;
            }

            skipValue();
            skipWhitespace();
            char c = peek();
            if (c == ',') { pos++; continue; }
            return null;
        }
        return null;
    }

    private char peek() {
        return pos < text.length() ? text.charAt(pos) : '\0';
    }

    private boolean expect(char c) {
        if (peek() != c) return false;
        pos++;
        return true;
    }

    private void skipWhitespace() {
        while (pos < text.length()) {
            char c = text.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') pos++;
            else break;
        }
    }

    private String readString() {
        if (!expect('"')) return null;
        StringBuilder sb = new StringBuilder(64);
        while (pos < text.length()) {
            char c = text.charAt(pos++);
            if (c == '"') return sb.toString();
            if (c != '\\') { sb.append(c); continue; }
            if (pos >= text.length()) return null;
            char escape = text.charAt(pos++);
            switch (escape) {
                case '"' -> sb.append('"');
                case '\\' -> sb.append('\\');
                case '/' -> sb.append('/');
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                case 't' -> sb.append('\t');
                case 'u' -> {
                    if (pos + 4 > text.length()) return null;
                    sb.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
                    pos += 4;
                }
                default -> sb.append(escape);
            }
        }
        return null;
    }

    /** Advances past the value at the cursor without building it. */
    private void skipValue() {
        char c = peek();
        switch (c) {
            case '"' -> skipStringFast();
            case '{' -> skipNested('{', '}');
            case '[' -> skipNested('[', ']');
            default -> {
                while (pos < text.length()) {
                    char n = text.charAt(pos);
                    if (n == ',' || n == '}' || n == ']') break;
                    pos++;
                }
            }
        }
    }

    private void skipStringFast() {
        pos++;
        while (pos < text.length()) {
            char c = text.charAt(pos++);
            if (c == '\\') { pos++; continue; }
            if (c == '"') return;
        }
    }

    private void skipNested(char open, char close) {
        int depth = 0;
        while (pos < text.length()) {
            char c = text.charAt(pos);
            if (c == '"') { skipStringFast(); continue; }
            pos++;
            if (c == open) depth++;
            else if (c == close) {
                depth--;
                if (depth == 0) return;
            }
        }
    }
}
