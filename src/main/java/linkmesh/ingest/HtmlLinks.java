package linkmesh.ingest;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pulls outbound links out of an HTML document.
 *
 * Regex rather than a DOM parser: a real parser handles broken markup better,
 * but anchors are the best-formed part of real HTML and a missed link costs one
 * edge out of millions. Not worth a dependency here.
 *
 * A base element, if present, overrides the page URL for relative resolution.
 */
public final class HtmlLinks {
    private HtmlLinks() {}

    private static final Pattern ANCHOR = Pattern.compile(
            "<a\\s[^>]*?href\\s*=\\s*(\"([^\"]*)\"|'([^']*)'|([^\\s\"'>]+))",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern BASE = Pattern.compile(
            "<base\\s[^>]*?href\\s*=\\s*(\"([^\"]*)\"|'([^']*)'|([^\\s\"'>]+))",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** Result of scanning one page: its canonical URL and the links it points at. */
    public record Page(String sourceUrl, List<String> targets) {}

    public static Page extract(String pageUrl, String html, boolean externalOnly) {
        URI base = UrlNormalizer.parseBase(pageUrl);
        String canonicalSource = UrlNormalizer.resolve(base, pageUrl);
        if (canonicalSource == null) return null;

        Matcher baseMatcher = BASE.matcher(html);
        if (baseMatcher.find()) {
            String declared = firstGroup(baseMatcher);
            String resolvedBase = UrlNormalizer.resolve(base, declared);
            if (resolvedBase != null) {
                URI candidate = UrlNormalizer.parseBase(resolvedBase);
                if (candidate != null) base = candidate;
            }
        }

        String sourceHost = UrlNormalizer.hostOf(canonicalSource);
        Set<String> targets = new LinkedHashSet<>();
        Matcher matcher = ANCHOR.matcher(html);
        while (matcher.find()) {
            String target = UrlNormalizer.resolve(base, decodeEntities(firstGroup(matcher)));
            if (target == null || target.equals(canonicalSource)) continue;
            if (externalOnly && UrlNormalizer.sameSite(sourceHost, UrlNormalizer.hostOf(target))) continue;
            targets.add(target);
        }
        return new Page(canonicalSource, new ArrayList<>(targets));
    }

    private static String firstGroup(Matcher matcher) {
        for (int group = 2; group <= 4; group++) {
            String value = matcher.group(group);
            if (value != null) return value;
        }
        return "";
    }

    /** Only the entities that actually appear inside href attributes. */
    private static String decodeEntities(String value) {
        if (value.indexOf('&') < 0) return value;
        return value.replace("&amp;", "&")
                    .replace("&#38;", "&")
                    .replace("&#x26;", "&")
                    .replace("&quot;", "\"")
                    .replace("&#39;", "'");
    }
}
