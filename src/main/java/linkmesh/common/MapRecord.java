package linkmesh.common;

/**
 * One emitted edge: page {@code sourceUrl} contains a link to {@code targetUrl},
 * so the reducer keyed on targetUrl gains sourceUrl as a backlink.
 */
public record MapRecord(String targetUrl, String sourceUrl) {}
