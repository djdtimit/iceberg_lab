"""Fault-injecting HTTP proxy (tests/storage/test_storage_faults.py: in front of RustFS;
tests/security/test_idp_outage.py: in front of a token endpoint).

Forwards every request unchanged (body kept, Host header kept unless ``host`` is given, so
SigV4 signatures stay valid) to ``upstream`` (default rustfs:9000). ``arm(mode, sources,
match)`` injects a fault for requests from the given source IPs (None: all) whose
"METHOD path?query body-start" matches the regex ``match`` (None: all):
    down   close the connection without an answer
    503    answer 503 SlowDown without forwarding
    slow   wait ``delay`` seconds, then forward
``hits`` lists the requests a fault hit; ``disarm()`` removes it.
"""

import http.client
import re
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class StorageProxy:
    """TCP/HTTP proxy in front of an S3-like service that can inject faults for chosen clients."""

    def __init__(self, port=19000, upstream=("rustfs", 9000), host=None):
        """``host``: Host header sent upstream (default: the client's, unchanged).

        Args:
            port: Port to listen on.
            upstream: ``(host, port)`` to forward to.
            host: Host header sent upstream (None: unchanged).
        """
        self.mode, self.sources, self.match, self.delay = None, None, None, 0
        self.hits = []
        self.log = None
        proxy = self

        class H(BaseHTTPRequestHandler):
            """Forward each request; apply the armed fault if it matches."""

            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                """Silence the request log.

                Args:
                    a: Log arguments (ignored).
                """

            def _go(self):
                """Forward one request (or fail/delay it) and relay the answer."""
                if "chunked" in (self.headers.get("transfer-encoding") or "").lower():
                    parts = []
                    while True:  # HTTP/1.1 chunked body: <hex size>\r\n<data>\r\n ... 0\r\n\r\n
                        size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
                        if size == 0:
                            while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                                pass
                            break
                        parts.append(self.rfile.read(size))
                        self.rfile.readline()
                    body = b"".join(parts)
                else:
                    n = int(self.headers.get("content-length") or 0)
                    body = self.rfile.read(n) if n else b""
                desc = f"{self.command} {self.path} {body[:200].decode(errors='replace')}"
                src = self.client_address[0]
                hit = (
                    proxy.mode
                    and (proxy.sources is None or src in proxy.sources)
                    and (proxy.match is None or re.search(proxy.match, desc))
                )
                if hit:
                    proxy.hits.append((proxy.mode, src, desc[:120]))
                    if proxy.mode == "down":
                        self.close_connection = True
                        try:
                            self.connection.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        return
                    if proxy.mode == "503":
                        out = b"<Error><Code>SlowDown</Code><Message>injected</Message></Error>"
                        self.send_response(503)
                        self.send_header("content-length", str(len(out)))
                        self.end_headers()
                        self.wfile.write(out)
                        return
                    if proxy.mode == "slow":
                        time.sleep(proxy.delay)
                c = http.client.HTTPConnection(*upstream, timeout=300)
                fwd = {
                    k: v
                    for k, v in self.headers.items()
                    if k.lower() not in ("transfer-encoding", "content-length")
                }
                fwd["Content-Length"] = str(len(body))
                if host:
                    fwd = {k: v for k, v in fwd.items() if k.lower() != "host"} | {"Host": host}
                c.request(self.command, self.path, body=body, headers=fwd)
                r = c.getresponse()
                data = r.read()
                if proxy.log is not None:
                    proxy.log.append(
                        (
                            self.command,
                            self.path[:80],
                            {
                                k: v
                                for k, v in self.headers.items()
                                if k.lower()
                                in (
                                    "content-length",
                                    "transfer-encoding",
                                    "content-encoding",
                                    "x-amz-content-sha256",
                                    "x-amz-decoded-content-length",
                                    "expect",
                                    "range",
                                )
                            },
                            len(body),
                            r.status,
                            len(data),
                            r.getheader("content-length"),
                            r.getheader("transfer-encoding"),
                        )
                    )
                self.send_response(r.status)
                for k, v in r.getheaders():
                    if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                        self.send_header(k, v)
                # HEAD: no body, but the object's size must reach the client unchanged
                length = (
                    r.getheader("content-length", "0") if self.command == "HEAD" else str(len(data))
                )
                self.send_header("content-length", length)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)

            do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _go

        self.server = ThreadingHTTPServer(("0.0.0.0", port), H)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def arm(self, mode, sources=None, match=None, delay=0):
        """Arm a fault.

        Args:
            mode: Fault mode (see module docstring).
            sources: Client IPs the fault applies to (None: all).
            match: Regex on the request line (None: all).
            delay: Seconds to delay (slow mode).
        """
        self.mode, self.sources, self.match, self.delay = mode, sources, match, delay
        self.hits = []

    def disarm(self):
        """Remove the fault."""
        self.mode = None

    def close(self):
        """Stop the proxy."""
        self.server.shutdown()
        self.server.server_close()
