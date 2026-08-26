#!/usr/bin/env python3
"""Builds a small WARC.GZ that exercises the tricky parts of link extraction.

Each record is its own gzip member, the way Common Crawl writes them, so the
reader's handling of concatenated members is covered too.
"""
import gzip
import io
import os
import sys

PAGES = [
    # Relative link, absolute link, the same target spelled four different ways,
    # two schemes that must be dropped, and an HTML entity in a query string.
    ("https://alpha.example/index.html", """
<html><head><title>Alpha</title></head><body>
<a href="/about">About us</a>
<a href="https://beta.example/home">Beta home</a>
<a href="https://beta.example/home#section">Same page, fragment</a>
<a href="HTTPS://BETA.EXAMPLE:443/home">Same page, uppercase and default port</a>
<a href="mailto:x@y.com">mail</a>
<a href="javascript:void(0)">js</a>
<a href='https://gamma.example/x?q=1&amp;r=2'>Gamma</a>
</body></html>"""),

    # base href changes what relative links resolve against.
    ("https://beta.example/home", """
<html><head><base href="https://beta.example/docs/"></head><body>
<a href="intro.html">Relative to base</a>
<a href="../top.html">Up one level</a>
<a href="https://alpha.example/index.html">Back to alpha</a>
</body></html>"""),

    ("https://gamma.example/x?q=1&r=2", """
<html><body>
<a href="https://alpha.example/index.html">alpha</a>
<a href="https://beta.example/home">beta</a>
</body></html>"""),

    # Not HTML: must be skipped entirely.
    ("https://alpha.example/logo.png", None),
]


def record(url, html):
    if html is None:
        body = b"\x89PNG\r\n\x1a\n" + b"\x00" * 40
        content_type = b"image/png"
    else:
        body = html.encode("utf-8")
        content_type = b"text/html; charset=utf-8"

    http = (b"HTTP/1.1 200 OK\r\nContent-Type: " + content_type
            + b"\r\nContent-Length: %d\r\n\r\n" % len(body) + body)

    header = (
        "WARC/1.0\r\n"
        "WARC-Type: response\r\n"
        f"WARC-Target-URI: {url}\r\n"
        "WARC-Date: 2024-01-01T00:00:00Z\r\n"
        "Content-Type: application/http; msgtype=response\r\n"
        f"Content-Length: {len(http)}\r\n"
        "\r\n"
    ).encode("utf-8")
    return header + http + b"\r\n\r\n"


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else "build/test.warc.gz"
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "wb") as out:
        for url, html in PAGES:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                gz.write(record(url, html))
            out.write(buf.getvalue())
    print(f"wrote {out_path} ({os.path.getsize(out_path)} bytes, {len(PAGES)} records)")


if __name__ == "__main__":
    main()
