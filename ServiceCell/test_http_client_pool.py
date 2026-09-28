#!/usr/bin/env python3
"""The http.client path of ExternalServiceExecutor (MUB_HTTP_CLIENT=http.client).

Drives the SHIPPED class, lifted from ExternalServiceExecutor.py by source (the
module imports grpc and mub_pb2, which a laptop may not have), against a local
HTTP/1.1 server that counts connections and requests:

  * status, body and headers come back as callers read them (headers.get ignores case);
  * kept-open connections are reused, and shared across threads;
  * a connection the server closed while idle is noticed before use and replaced --
    the request is sent once, never retried;
  * a response that says Connection: close is not kept.
"""
import http.server, os, re, select, socketserver, sys, threading, time
import http.client
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
src = open(os.path.join(HERE, "ExternalServiceExecutor.py")).read()
a = src.index("class _HCResponse:"); b = src.index("def _http_client_choice():")
ns = {"http": http, "select": select, "urlsplit": urlsplit, "threading": threading}
exec(compile(src[a:b], "ExternalServiceExecutor.py", "exec"), ns)
Pool = ns["_HttpClientPool"]

state = {"conns": 0, "reqs": 0, "posts": []}
lock = threading.Lock()

class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def setup(self):
        super().setup()
        with lock: state["conns"] += 1
    def log_message(self, *a): pass
    def _reply(self, body, close=False):
        with lock: state["reqs"] += 1
        b = body.encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(b)))
        self.send_header("X-Macaw-Out-Request-Us", "12345")
        if close: self.send_header("Connection", "close")
        self.end_headers(); self.wfile.write(b)
        if close: self.close_connection = True
    def do_GET(self):
        self._reply("get " + self.path, close=self.path.endswith("close"))
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0)); body = self.rfile.read(n).decode()
        with lock: state["posts"].append(body)
        self._reply("post " + self.path + " " + body + " " + self.headers.get("cosched-fanout", "-"))

class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
srv = S(("127.0.0.1", 0), H); port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{port}"
fails = 0
def check(ok, what):
    global fails; print(("PASS  " if ok else "FAIL  ") + what); fails += 0 if ok else 1

p = Pool(16)
r = p.post(base + "/api/v1", data='{"a": 1}', headers={"Content-type": "application/json", "cosched-fanout": "3"})
check(r.status_code == 200 and r.text == 'post /api/v1 {"a": 1} 3', f"POST: status, body and a request header arrive ({r.text!r})")
check(r.headers.get("x-macaw-out-request-us") == "12345", "response header read case-insensitively, as callers do")
r = p.get(base + "/api/v1?x=1")
check(r.text == "get /api/v1?x=1", "GET with a query string")
check(state["conns"] == 1 and state["reqs"] == 2, f"the second call reused the kept connection (connections {state['conns']}, requests {state['reqs']})")

# the server closes an idle kept connection: noticed before use, one new connection, one request
with p.lock:
    c = p.idle[("127.0.0.1", port)][0]
    import socket; c.sock.shutdown(socket.SHUT_RDWR)   # stands in for the far side's idle timeout
before = dict(state, posts=len(state["posts"]))
r = p.post(base + "/after-close", data="x")
check(r.status_code == 200 and state["conns"] == before["conns"] + 1 and len(state["posts"]) == before["posts"] + 1,
      "a connection closed while idle is replaced before use; the request is sent once")

r = p.get(base + "/please-close")
check(r.status_code == 200 and not p.idle.get(("127.0.0.1", port)), "a Connection: close response is not kept")

# threads, as run_external_service_ms_trace does (a new ThreadPoolExecutor per request)
state.update(conns=0, reqs=0); state["posts"].clear()
p2 = Pool(16)
for rnd in range(20):
    with ThreadPoolExecutor(4) as ex:
        rs = list(ex.map(lambda i: p2.post(base + f"/t{i}", data=str(i)), range(4)))
    assert all(x.status_code == 200 for x in rs)
check(state["reqs"] == 80 and len(state["posts"]) == 80, f"80 calls from 20 short-lived pools of 4 threads: 80 requests, none twice ({state['reqs']})")
check(state["conns"] <= 8, f"connections are shared across those threads and reused (opened {state['conns']} for 80 calls)")

# timing, for the record (loopback, no sidecar)
t0 = time.perf_counter()
for i in range(500): p2.post(base + "/t", data="x")
print(f"      http.client pool: {(time.perf_counter() - t0) / 500 * 1e3:.3f} ms per call on loopback")
srv.shutdown()
print(("OK" if not fails else "FAILED") + f": {fails} failing check(s)")
sys.exit(1 if fails else 0)
