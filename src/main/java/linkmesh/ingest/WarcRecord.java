package linkmesh.ingest;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;

/**
 * One WARC record, already split into its WARC headers, the HTTP headers of the
 * captured response, and the response body.
 */
public record WarcRecord(Map<String, String> warcHeaders,
                         Map<String, String> httpHeaders,
                         byte[] payload) {

    public String type() { return warcHeaders.getOrDefault("warc-type", ""); }

    /**
     * The URL this page was fetched from.
     *
     * This single header is the reason WARC is the right input format. A bare
     * .html file on disk has lost the address it came from, and without it a
     * relative href like "/about" cannot be resolved into anything meaningful.
     */
    public String targetUri() { return warcHeaders.get("warc-target-uri"); }

    public boolean isResponse() { return "response".equals(type()); }

    public String contentType() {
        return httpHeaders.getOrDefault("content-type", "").toLowerCase();
    }

    public boolean isHtml() {
        String type = contentType();
        return type.contains("text/html") || type.contains("application/xhtml");
    }

    /** Decodes the body using the charset the server declared, falling back to UTF-8. */
    public String text() {
        Charset charset = StandardCharsets.UTF_8;
        String contentType = contentType();
        int index = contentType.indexOf("charset=");
        if (index >= 0) {
            String name = contentType.substring(index + 8).trim();
            int semicolon = name.indexOf(';');
            if (semicolon >= 0) name = name.substring(0, semicolon);
            name = name.replace("\"", "").trim();
            try {
                if (!name.isEmpty() && Charset.isSupported(name)) charset = Charset.forName(name);
            } catch (IllegalCharsetNameException | UnsupportedCharsetException ignored) {
                // Malformed charset declarations are common in the wild; UTF-8 is
                // a safe fallback because href values are almost always ASCII.
            }
        }
        return new String(payload, charset);
    }
}
