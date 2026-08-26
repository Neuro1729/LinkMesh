package linkmesh.ingest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * Resolves a link and canonicalizes it into the key the reduce groups on.
 *
 * Without this, Example.com/x, example.com/x, example.com:80/x and
 * example.com/x#top are four different keys and every count is wrong, in a way
 * nothing fails loudly about.
 *
 * Applied: relative resolution, http/https only, lowercase scheme and host, drop
 * fragment, drop default port, empty path becomes "/".
 *
 * Not applied: trailing-slash stripping, query reordering, dropping "www.".
 * Each of those can point at a genuinely different page on some sites.
 */
public final class UrlNormalizer {
    private UrlNormalizer() {}

    public static URI parseBase(String url) {
        try {
            URI uri = new URI(url.trim());
            return uri.isAbsolute() ? uri : null;
        } catch (URISyntaxException e) {
            return null;
        }
    }

    /** Returns the canonical absolute form of href, or null if it is not a usable web link. */
    public static String resolve(URI base, String href) {
        if (href == null) return null;
        String trimmed = href.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) return null;

        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (lower.startsWith("javascript:") || lower.startsWith("mailto:") || lower.startsWith("tel:")
                || lower.startsWith("data:") || lower.startsWith("about:") || lower.startsWith("ftp:")) {
            return null;
        }

        try {
            URI resolved = base == null ? new URI(trimmed) : base.resolve(trimmed);
            if (!resolved.isAbsolute()) return null;

            String scheme = resolved.getScheme();
            if (scheme == null) return null;
            scheme = scheme.toLowerCase(Locale.ROOT);
            if (!scheme.equals("http") && !scheme.equals("https")) return null;

            String host = resolved.getHost();
            if (host == null || host.isEmpty()) return null;
            host = host.toLowerCase(Locale.ROOT);

            int port = resolved.getPort();
            if ((scheme.equals("http") && port == 80) || (scheme.equals("https") && port == 443)) port = -1;

            String path = resolved.getPath();
            if (path == null || path.isEmpty()) path = "/";
            String query = resolved.getQuery();
            if (query != null && query.isEmpty()) query = null;

            // Rebuild through URI rather than string concatenation. getPath()
            // returns the decoded path, so a title like "New%20York" comes back
            // holding a real space, and non-ASCII titles come back as raw
            // characters. Reassembling this way re-encodes both, which keeps
            // every emitted key printable ASCII with no whitespace in it.
            String result = new URI(scheme, null, host, port, path, query, null).toASCIIString();
            // A control character here would corrupt the .page line format, and
            // any URL containing one is malformed anyway.
            for (int i = 0; i < result.length(); i++) {
                char c = result.charAt(i);
                if (c == '\n' || c == '\r' || c == '\t' || c < 0x20) return null;
            }
            return result.length() > 2000 ? null : result;
        } catch (URISyntaxException | IllegalArgumentException e) {
            return null;
        }
    }

    public static String hostOf(String url) {
        try {
            String host = new URI(url).getHost();
            return host == null ? "" : host.toLowerCase(Locale.ROOT);
        } catch (URISyntaxException e) {
            return "";
        }
    }

    /** Compares hosts ignoring a leading www, used by the same-site link filter. */
    public static boolean sameSite(String hostA, String hostB) {
        return stripWww(hostA).equals(stripWww(hostB));
    }

    private static String stripWww(String host) {
        return host.startsWith("www.") ? host.substring(4) : host;
    }
}
