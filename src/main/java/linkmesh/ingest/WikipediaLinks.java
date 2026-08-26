package linkmesh.ingest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Set;

/**
 * Which links in a Wikipedia article count as article-to-article edges.
 *
 * Drops red links (/wiki/Title?action=edit&amp;redlink=1), which point at pages
 * that do not exist and would inflate the key count.
 *
 * Drops non-article namespaces (Category, File, Template, Talk). They live under
 * /wiki/ too and every article links to several, so left in they dominate the
 * top of the ranking.
 */
public final class WikipediaLinks {
    private WikipediaLinks() {}

    private static final Set<String> NON_ARTICLE_NAMESPACES = Set.of(
            "file", "image", "media", "category", "template", "help", "special",
            "talk", "user", "wikipedia", "project", "portal", "module",
            "mediawiki", "draft", "book", "timedtext", "gadget");

    /** True if the URL names a normal article on the same wiki. */
    public static boolean isArticle(String url) {
        try {
            URI uri = new URI(url);
            if (uri.getQuery() != null) return false;

            String path = uri.getPath();
            if (path == null || !path.startsWith("/wiki/")) return false;

            String title = path.substring("/wiki/".length());
            if (title.isEmpty()) return false;

            int colon = title.indexOf(':');
            if (colon > 0) {
                String prefix = decodePrefix(title.substring(0, colon));
                if (NON_ARTICLE_NAMESPACES.contains(prefix)) return false;
            }
            return true;
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private static String decodePrefix(String prefix) {
        return prefix.replace('_', ' ').trim().toLowerCase(Locale.ROOT);
    }

    /** Readable article title, for reporting rather than for keys. */
    public static String titleOf(String url) {
        int index = url.indexOf("/wiki/");
        if (index < 0) return url;
        return java.net.URLDecoder.decode(url.substring(index + 6), java.nio.charset.StandardCharsets.UTF_8)
                .replace('_', ' ');
    }
}
