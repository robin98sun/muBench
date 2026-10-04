#!/usr/bin/env python3
"""X-Mub-Call-Tails: what each call's deadline was built from (co-scheduling, 2026-10-03).

Drives the SHIPPED request_external_service_ms_trace, lifted from
ExternalServiceExecutor.py by source (the module imports grpc and mub_pb2, which a
laptop may not have), with request_function replaced by a stub that returns the
headers a Macaw outbound sidecar would:

  * a call whose response carries x-macaw-out-{deadline,tail,net}-us gives one record
    "callee,task index,deadline,tail,net", in that order;
  * a tail of 0 (no model) is recorded as 0, not dropped;
  * a call with any of the three missing or not a number gives no record (bypassing
    and FIFO write none) -- and its X-Mub-Calls record is still written, unchanged;
  * no headers at all (gRPC) gives no record and no error;
  * call_tail_log=None (an old caller) records nothing and changes nothing else.
And, by source, that CellController-mp passes the list down and returns it as
X-Mub-Call-Tails only when it is not empty.
"""
import os, re, time, types

HERE = os.path.dirname(os.path.abspath(__file__))
src = open(os.path.join(HERE, "ExternalServiceExecutor.py")).read()
a = src.index("CALL_STAMP_HEADERS = "); b = src.index("# external_services is a 2-dimensional list")
ns = {"time": time}
exec(compile(src[a:b], "ExternalServiceExecutor.py", "exec"), ns)

REPLIES = {}
def request_function(service_name, *args, **kw):
    return types.SimpleNamespace(headers=REPLIES[service_name], text="ok", status_code=200)
ns["request_function"] = request_function

class Log:
    def info(self, *a): pass
    def error(self, *a): pass
app = types.SimpleNamespace(logger=Log())

fails = 0
def check(ok, what):
    global fails; print(("PASS  " if ok else "FAIL  ") + what); fails += 0 if ok else 1

FULL = {"x-macaw-out-request-us": "100", "x-macaw-out-response-us": "200",
        "x-macaw-out-deadline-us": "1790000000050000", "x-macaw-out-tail-us": "61234",
        "x-macaw-out-net-us": "900"}
REPLIES.update({
    "s-full": FULL,
    "s-nomodel": dict(FULL, **{"x-macaw-out-tail-us": "0"}),
    "s-nodeadline": {"x-macaw-out-request-us": "100", "x-macaw-out-response-us": "200"},
    "s-partial": {k: v for k, v in FULL.items() if k != "x-macaw-out-net-us"},
    "s-garbage": dict(FULL, **{"x-macaw-out-tail-us": "-5"}),
    "s-grpc": None,
})
def run(names, tails):
    calls = []
    series = [{"name": n, "input": {}} for n in names]
    ns["request_external_service_ms_trace"](series, 3, None, None, None, "", app, None,
                                            extra_headers={}, call_log=calls, call_tail_log=tails)
    return calls

tails = []
calls = run(["s-full"], tails)
check(tails == ["s-full,3,1790000000050000,61234,900"], f"full: one record, callee,task,deadline,tail,net: {tails}")
check(len(calls) == 1 and len(calls[0].split(",")) == 11, "full: the X-Mub-Calls record still has its 11 fields")

tails = []; run(["s-nomodel"], tails)
check(tails == ["s-nomodel,3,1790000000050000,0,900"], f"no model: tail 0 is recorded: {tails}")

for n in ("s-nodeadline", "s-partial", "s-garbage", "s-grpc"):
    tails = []; calls = run([n], tails)
    check(tails == [] and len(calls) == 1, f"{n}: no tail record, the call record is still there")

tails = []; calls = run(["s-full", "s-nodeadline", "s-nomodel"], tails)
check([t.split(",")[0] for t in tails] == ["s-full", "s-nomodel"] and len(calls) == 3,
      "a series: one tail record per call that carried a deadline, in call order")

calls = run(["s-full"], None)
check(len(calls) == 1, "call_tail_log=None: no error, the call record unchanged")

cc = open(os.path.join(HERE, "CellController-mp.py")).read()
check("call_tail_log=call_tail_log" in cc, "CellController-mp passes call_tail_log down")
check(re.search(r"if call_tail_log:\s*\n\s*response\.headers\['X-Mub-Call-Tails'\] = ';'\.join\(call_tail_log\)", cc) is not None,
      "CellController-mp returns X-Mub-Call-Tails, only when not empty")

print(f"test_call_tails: {fails} failed")
raise SystemExit(1 if fails else 0)
