package linkmesh.proto;

import java.net.InetSocketAddress;

/** A reachable network address. Parsed from "host:port". */
public record Endpoint(String host, int port) {

    public static Endpoint parse(String text) {
        if (text == null || text.isBlank()) throw new IllegalArgumentException("empty endpoint");
        int colon = text.lastIndexOf(':');
        if (colon < 0) throw new IllegalArgumentException("endpoint must be host:port, got: " + text);
        String host = text.substring(0, colon).trim();
        int port = Integer.parseInt(text.substring(colon + 1).trim());
        if (host.isEmpty()) throw new IllegalArgumentException("endpoint missing host: " + text);
        if (port < 1 || port > 65535) throw new IllegalArgumentException("port out of range: " + port);
        return new Endpoint(host, port);
    }

    public static Endpoint parse(String text, int defaultPort) {
        if (text == null || text.isBlank()) throw new IllegalArgumentException("empty endpoint");
        return text.lastIndexOf(':') < 0
                ? new Endpoint(text.trim(), defaultPort)
                : parse(text);
    }

    public InetSocketAddress toSocketAddress() {
        return new InetSocketAddress(host, port);
    }

    @Override
    public String toString() { return host + ":" + port; }
}
