package linkmesh.proto;

import linkmesh.common.Text;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A verb, string headers, and an optional binary body.
 *
 * Wire format is a header line followed by raw bytes, with the body length
 * carried in the _len header. The header stays greppable in a packet dump while
 * the body moves archives and shuffle batches with no base64 inflation.
 */
public final class Message {
    public static final String LEN = "_len";

    private final String verb;
    private final Map<String, String> headers;
    private final byte[] body;

    private static final byte[] NO_BODY = new byte[0];

    public Message(String verb, Map<String, String> headers, byte[] body) {
        this.verb = verb;
        this.headers = headers == null ? new LinkedHashMap<>() : headers;
        this.body = body == null ? NO_BODY : body;
    }

    /** Builds a message from alternating key/value pairs. */
    public static Message of(String verb, String... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("headers must be key/value pairs");
        }
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            if (keyValues[i + 1] != null) headers.put(keyValues[i], keyValues[i + 1]);
        }
        return new Message(verb, headers, NO_BODY);
    }

    public Message with(String key, String value) {
        Map<String, String> copy = new LinkedHashMap<>(headers);
        if (value == null) copy.remove(key); else copy.put(key, value);
        return new Message(verb, copy, body);
    }

    public Message with(String key, long value) {
        return with(key, Long.toString(value));
    }

    public Message withBody(byte[] newBody) {
        return new Message(verb, new LinkedHashMap<>(headers), newBody);
    }

    public Message withBody(String newBody) {
        return withBody(newBody.getBytes(StandardCharsets.UTF_8));
    }

    public String verb() { return verb; }

    public Map<String, String> headers() { return headers; }

    public byte[] body() { return body; }

    public String bodyText() { return new String(body, StandardCharsets.UTF_8); }

    public boolean hasBody() { return body.length > 0; }

    public String get(String key) { return headers.get(key); }

    public String get(String key, String fallback) {
        String value = headers.get(key);
        return value == null ? fallback : value;
    }

    public String require(String key) {
        String value = headers.get(key);
        if (value == null) throw new IllegalArgumentException(verb + " missing header: " + key);
        return value;
    }

    public int getInt(String key, int fallback) {
        String value = headers.get(key);
        return value == null ? fallback : Integer.parseInt(value);
    }

    public long getLong(String key, long fallback) {
        String value = headers.get(key);
        return value == null ? fallback : Long.parseLong(value);
    }

    public boolean getBool(String key, boolean fallback) {
        String value = headers.get(key);
        return value == null ? fallback : Boolean.parseBoolean(value);
    }

    public boolean isOk() { return Verbs.OK.equals(verb); }

    public boolean isError() { return Verbs.ERR.equals(verb); }

    /** Throws if this is an ERR reply, otherwise returns itself. Call sites read cleanly. */
    public Message orThrow() {
        if (isError()) throw new ProtocolException(get("reason", "remote error"));
        return this;
    }

    public static Message ok() { return of(Verbs.OK); }

    public static Message ok(String... keyValues) { return of(Verbs.OK, keyValues); }

    public static Message error(String reason) { return of(Verbs.ERR, "reason", reason); }

    /** Serializes the header line. Body is written separately by the codec. */
    String encodeHeader() {
        StringBuilder sb = new StringBuilder(64);
        sb.append(verb);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (LEN.equals(entry.getKey())) continue;
            sb.append('\t').append(entry.getKey()).append('=').append(Text.escape(entry.getValue()));
        }
        if (body.length > 0) sb.append('\t').append(LEN).append('=').append(body.length);
        return sb.toString();
    }

    static Message decodeHeader(String line) {
        String[] parts = line.split("\t", -1);
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 1; i < parts.length; i++) {
            if (parts[i].isEmpty()) continue;
            int eq = parts[i].indexOf('=');
            if (eq < 0) { headers.put(parts[i], "true"); continue; }
            headers.put(parts[i].substring(0, eq), Text.unescape(parts[i].substring(eq + 1)));
        }
        return new Message(parts[0], headers, NO_BODY);
    }

    @Override
    public String toString() {
        return encodeHeader().replace('\t', ' ');
    }
}
