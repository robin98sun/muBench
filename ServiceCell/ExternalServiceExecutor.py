import os
import random
from readline import append_history_file
import requests
from concurrent.futures import ThreadPoolExecutor, wait, as_completed, FIRST_COMPLETED
import time
import threading
import grpc
import mub_pb2_grpc as pb2_grpc
import mub_pb2 as pb2
import json
from pprint import pprint
from requests.adapters import HTTPAdapter
import http.client
import select
from urllib.parse import urlsplit


service_stub = dict()
s = None
_rest_session = None
_rest_session_pid = None
_rest_session_lock = threading.Lock()


def _clamp(value, lower_bound, upper_bound):
    return max(lower_bound, min(upper_bound, value))


def _parse_positive_int(raw_value, default_value):
    try:
        parsed_value = int(raw_value)
        if parsed_value > 0:
            return parsed_value
    except (TypeError, ValueError):
        pass
    return default_value


def _parse_bool(raw_value, default_value):
    if raw_value is None:
        return default_value

    normalized_value = str(raw_value).strip().lower()
    if normalized_value in {"1", "true", "yes", "on"}:
        return True
    if normalized_value in {"0", "false", "no", "off"}:
        return False
    return default_value


def _unique_downstream_host_count(work_model):
    unique_hosts = set()
    for service_config in work_model.values():
        if isinstance(service_config, dict):
            host = service_config.get("url")
            if host:
                unique_hosts.add(host)
    return len(unique_hosts)


def _build_rest_session(work_model, app):
    unique_hosts = _unique_downstream_host_count(work_model)
    default_pool_connections = _clamp(unique_hosts, 32, 256)
    pool_connections = _parse_positive_int(
        os.getenv("SERVICE_POOL_CONNECTIONS"),
        default_pool_connections,
    )
    pool_maxsize = _parse_positive_int(os.getenv("SERVICE_POOL_MAXSIZE"), 16)
    pool_block = _parse_bool(os.getenv("SERVICE_POOL_BLOCK"), False)

    session = requests.Session()
    # trust_env=False: do not re-read the environment (proxies, .netrc, CA
    # bundle) on every request. With Kubernetes service links on, a pod has
    # ~1,400 env vars and requests scanned them twice per call -- 25 ms of CPU
    # before each call left the process (robin98-317986, 2026-09-27; raw
    # socket 0.1 ms, trust_env=False 1.3 ms). No proxy is used in the cluster.
    session.trust_env = False
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=0,
        pool_block=pool_block,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    app.logger.info(
        "Configured REST session for pid %s with unique_hosts=%d, pool_connections=%d, pool_maxsize=%d, pool_block=%s",
        os.getpid(),
        unique_hosts,
        pool_connections,
        pool_maxsize,
        pool_block,
    )
    return session


class _HCResponse:
    """What callers read from a response: status_code, text, headers.get()
    (case-insensitive, like requests)."""
    __slots__ = ("status_code", "text", "headers")


class _HttpClientPool:
    """The same calls as a requests.Session (post/get with data= and headers=),
    made with http.client over kept-open connections.

    Robin, 2026-09-27: measured inside a pod on robin98-317986, a call reaches
    the sidecar 0.2 ms after http.client sends it and 1.2 ms after a
    requests.Session (trust_env=False) does -- the rest is requests/urllib3 work
    in the interpreter. MUB_HTTP_CLIENT picks the path, so the two can be
    compared on the same deployment.

    One pool for the process, shared by every thread under a lock: the calls
    run on a ThreadPoolExecutor made per request, so a per-thread connection
    would never be reused. Like the requests adapter here (max_retries=0),
    nothing is sent twice: a kept connection the far side has closed is found
    before it is used (readable while idle = EOF), never by a failed send."""

    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.idle = {}              # (host, port) -> [HTTPConnection]
        self.lock = threading.Lock()

    def _take(self, key):
        with self.lock:
            conns = self.idle.get(key)
            while conns:
                c = conns.pop()
                sock = c.sock
                if sock is None:
                    continue
                try:
                    r, _, _ = select.select([sock], [], [], 0)
                except (OSError, ValueError):
                    r = [sock]
                if r:                  # closed (or unexpected bytes) while idle
                    c.close()
                    continue
                return c
        return http.client.HTTPConnection(key[0], key[1])

    def _give(self, key, c):
        with self.lock:
            conns = self.idle.setdefault(key, [])
            if len(conns) < self.maxsize:
                conns.append(c)
                return
        c.close()

    def _call(self, method, url, data=None, headers=None):
        u = urlsplit(url)
        key = (u.hostname, u.port or 80)
        path = (u.path or "/") + (("?" + u.query) if u.query else "")
        body = data.encode("utf-8") if isinstance(data, str) else data
        c = self._take(key)
        try:
            c.request(method, path, body=body, headers=headers or {})
            resp = c.getresponse()
            payload = resp.read()
        except Exception:
            c.close()
            raise
        r = _HCResponse()
        r.status_code = resp.status
        r.text = payload.decode("utf-8", "replace")
        r.headers = resp.headers      # email.message.Message: .get() ignores case
        if resp.will_close:
            c.close()
        else:
            self._give(key, c)
        return r

    def post(self, url, data=None, headers=None):
        return self._call("POST", url, data=data, headers=headers)

    def get(self, url, headers=None):
        return self._call("GET", url, headers=headers)


def _http_client_choice():
    v = (os.getenv("MUB_HTTP_CLIENT") or "requests").strip().lower()
    return "http.client" if v in ("http.client", "httpclient", "http_client") else "requests"


def _get_rest_session(work_model, app):
    global _rest_session, _rest_session_pid
    current_pid = os.getpid()
    if _rest_session is not None and _rest_session_pid == current_pid:
        return _rest_session

    with _rest_session_lock:
        if _rest_session is None or _rest_session_pid != current_pid:
            if _http_client_choice() == "http.client":
                maxsize = _parse_positive_int(os.getenv("SERVICE_POOL_MAXSIZE"), 16)
                _rest_session = _HttpClientPool(maxsize)
                app.logger.info("Configured http.client pool for pid %s (MUB_HTTP_CLIENT), maxsize=%d",
                                current_pid, maxsize)
            else:
                _rest_session = _build_rest_session(work_model, app)
            _rest_session_pid = current_pid
    return _rest_session

def init_REST(app):
    app.logger.info("Init REST function")
    global request_function
    request_function = request_REST

def init_gRPC(my_service_graph, workmodel, server_port, app):
    app.logger.info("Init gRPC function")
    global service_stub, request_function
    request_function = request_gRPC

    for group in my_service_graph:
        for service in group["services"]:
            host = f'{workmodel[service]["url"]}'
            # instantiate a channel
            channel = grpc.insecure_channel(
                '{}:{}'.format(host, server_port))
            # bind the client and the server
            service_stub[service] = pb2_grpc.MicroServiceStub(channel)

def request_REST(service,id,work_model,s,trace,query_string, app, jaeger_context, ms_trace_input=None, request_headers=None):
    try:
        service_no_escape = service.split("__")[0]
        session = _get_rest_session(work_model, app)
        if ms_trace_input:
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            headers.update(jaeger_context)
            if request_headers:
                for key, value in request_headers.items():
                    headers[key] = value
            json_payload = json.dumps(ms_trace_input)
            app.logger.debug(f'Requesting external service via REST: {service}, headers: {headers}')
            return session.post(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}',data=json_payload,headers=headers)

        elif len(trace)==0 and len(query_string)==0:
            # default 
            return session.get(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}', headers=jaeger_context)
        elif len(trace)>0:
            # trace-driven request
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            headers.update(jaeger_context)
            json_dict = dict()
            json_dict[service] = trace[id][service]
            json_payload = json.dumps(json_dict)
            if  len(query_string)==0:
                return session.post(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}',data=json_payload,headers=headers)
            else:
                return session.post(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}?{query_string}',data=json_payload,headers=headers)
        elif  len(query_string)>0:
            # request with enclosed behaviour information
            return session.get(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}?{query_string}', headers=jaeger_context)  
        else:
            r = requests.Response()
            r.status_code = 505
            return r
    except Exception as err:
        app.logger.error("Error in request external service %s -- %s" % (service, str(err)))
        r = requests.Response()
        r.status_code = 505
        return r

def request_gRPC(service,id,work_model,s,trace,query_string,app, trace_context=None, ms_trace_input=None):
    app.logger.debug(f'Requesting external service via gRPC: {service}')
    message = None
    if ms_trace_input:
        json_payload = json.dumps(ms_trace_input)
        message = pb2.Message(message=json_payload)
    else:
        message = pb2.Message(message=f"Hello service: {service}")
    response = service_stub[service].GetMicroServiceResponse(message)
    return response


def external_service(group,id,work_model,trace,query_string, app, trace_context):
    app.logger.info("**** Start SERVICES in thread: %s" % str(group))
    global request_function
    if group["seq_len"] < len(group["services"]):
        # Randomly select seq_len elements from services in the group
        selected_services = random.sample(group["services"], k=group["seq_len"])
    else:
        selected_services = group["services"]

    # read probabilities of services of the group, if exist
    if "probabilities" in  group.keys():
        probabilities = group["probabilities"]
    else:
        probabilities = dict()
    
    service_error_dict = dict()
    service_error_flag = False

    for service in selected_services:
        # sleep_time = random.randint(2, 5)
        # app.logger.info("**** Service: %s -- Sleep for %d" % (service, sleep_time))
        # time.sleep(sleep_time)
        try:
            # "url": "http://s0.default.svc.cluster.local",
            # "path": "/api/v1",
            if service in probabilities.keys():
                p = probabilities[service]
            else:
                p = 1
            if random.random() < p :
                # service called with probability p
                r = request_function(service,id,work_model,s,trace,query_string, app, trace_context)
                app.logger.info("Service: %s -> Status_code: %s -- len(text): %d" % (service, r.status_code, len(r.text)))
                if type(r.status_code) == bool and not r.status_code:
                    raise Exception(f"Error in external service: {service} -- (gRPC) status_code: {r.status_code}")
                elif type(r.status_code) == int and r.status_code != 200:
                    raise Exception(f"Error in external service: {service} -- (REST) status_code: {r.status_code}")

        except Exception as err:
            service_error_dict[service] = err
            service_error_flag = True
            app.logger.error("Error in request external service %s -- %s" % (service, str(err)))

    app.logger.info("#### SERVICE Done!")
    return service_error_flag, service_error_dict


def run_external_service(services_group, work_model, query_string, trace, app, trace_context=None):
    
    app.logger.info("** EXTERNAL SERVICES")
    service_error_dict = dict()
    number_of_groups = len(services_group)
    pool = ThreadPoolExecutor(number_of_groups)
    futures = list()
    id = 0
    for group in services_group:
        futures.append(pool.submit(external_service, group, id, work_model, trace, query_string, app, trace_context))
        id = id + 1
    wait(futures)
    for x in as_completed(futures):
        if x.result()[0]:
            service_error_dict.update(x.result()[1])
    app.logger.info("--------> Threads Done!")
    return service_error_dict


# for MS TRACE, the service_series is a list of services to be called in sequence
# PER-REQUEST PATH STAMPS (co-scheduling, 2026-09-25). One record per downstream
# call, wall-clock microseconds, so the caller's time outside its own handler can
# be divided into segments: its sidecar out, the network, the callee's sidecar
# (arrival -> Macaw release), the callee's handler, and back. The x-macaw-*
# values are written by the Macaw Envoy module on the RESPONSE -- the callee's
# inbound sidecar (in-*) and this pod's outbound sidecar (out-*) -- and the
# x-mub-handler-* by the callee's own handler. Fields, in order:
#   callee, task index, sent, received (this process, around the HTTP call),
#   callee handler start, callee handler end,
#   callee sidecar arrival, release, response,
#   caller sidecar request, response
# An absent stamp is an empty field, never 0 or a guess.
CALL_STAMP_HEADERS = ('x-mub-handler-start-us', 'x-mub-handler-end-us',
                      'x-macaw-in-arrival-us', 'x-macaw-in-release-us', 'x-macaw-in-response-us',
                      'x-macaw-out-request-us', 'x-macaw-out-response-us')


def _call_stamp_record(service_name, task_index, sent_us, received_us, headers):
    fields = [service_name, str(task_index), str(sent_us), str(received_us)]
    for h in CALL_STAMP_HEADERS:
        v = headers.get(h) if headers is not None else None
        fields.append(v if (v is not None and v.isdigit()) else '')
    return ','.join(fields)


def request_external_service_ms_trace(service_series, id, work_model, s, trace, query_string, app, trace_context, extra_headers=None, call_log=None):
    app.logger.info("**** Start SERVICES in thread: %s (via MS TRACE)" % str([service["name"] for service in service_series]))
    start_time = time.time()
    service_error_dict = dict()
    service_response_dict = dict()
    service_error_flag = False
    for service in service_series:
        if "name" not in service or "input" not in service:
            continue
        service_name = service["name"]
        service_input = service["input"]
        service_input["trace_type"] = "ms-trace"
        try:
            _sent_us = int(time.time() * 1_000_000)
            r = request_function(service_name,id,work_model,s,trace,query_string, app, trace_context, ms_trace_input=service_input, request_headers=extra_headers)
            _received_us = int(time.time() * 1_000_000)
            if call_log is not None:
                # list.append is atomic in CPython; the parallel branches share call_log
                call_log.append(_call_stamp_record(service_name, id, _sent_us, _received_us,
                                                   getattr(r, 'headers', None)))
            if len(r.text) < 100:
                app.logger.info("Service: %s -> Status_code: %s -- text: %s" % (service_name, r.status_code, r.text))
            else:
                app.logger.info("Service: %s -> Status_code: %s -- len(text): %d" % (service_name, r.status_code, len(r.text)))
            if type(r.status_code) == bool and not r.status_code:
                raise Exception(f"grpc error: {r.status_code}")
            elif type(r.status_code) == int and r.status_code != 200:
                raise Exception(f"rest error: {r.status_code}")
            else:
                service_response_dict[service_name] = f"{service_name}::{(time.time()-start_time)*1000}::{r.text}"
        except Exception as err:
            app.logger.error("Error in request external service %s -- %s" % (service_name, str(err)))
            service_error_dict[service_name] = err
            service_error_flag = True
    app.logger.info("#### SERVICE Done!")
    return service_error_flag, service_error_dict, service_response_dict

# external_services is a 2-dimensional list
# the first dimension is concurrent service series, which are run in parallel
# the second dimension, i.e., in each service series, are the services to be called in sequence
def run_external_service_ms_trace(external_services, work_model, query_string, trace, app, trace_context=None, request_headers=None, call_log=None):
    app.logger.info("** EXTERNAL SERVICES (via MS TRACE)")
    service_error_dict = dict()
    service_response_dict = dict()
    number_of_groups = len(external_services)

    extra_headers = dict()
    if request_headers:
        for key, value in request_headers.items():
            extra_headers[key] = value

    pool = ThreadPoolExecutor(number_of_groups)
    futures = list()
    id = 0
    fanout = len(external_services)
    extra_headers["cosched-fanout"] = f"{fanout}"
    # Ordered list (one entry per parallel branch), NOT a set: a set deduplicates
    # branches that call the same service, making len(callee-set) < cosched-fanout and
    # undercounting k in the WASM tail predictor (evk_from_pod_list). Keep duplicates so
    # the non-blocking path's k matches the true fanout.
    fanout_callee_list = []
    for service_series in external_services:
        for service in service_series:
            fanout_callee_list.append(service["name"])
            # only the first service is counted as callee for fanout, since they are called in sequence in the same service series, and the downstream services in the same series will not cause extra fanout
            break
    extra_headers["cosched-query-callee-set"] = f"{','.join(fanout_callee_list)}"
    for service_series in external_services:
        copy_extra_headers = extra_headers.copy()
        copy_extra_headers["cosched-task-index"] = f"{id}"
        app.logger.debug(f'sending external service request for query {extra_headers["cosched-query-id"]} which fanout is {fanout} and task index is {id}')
        futures.append(pool.submit(request_external_service_ms_trace, service_series, id, work_model, s, trace, query_string, app, trace_context, copy_extra_headers, call_log))
        id = id + 1
    wait(futures)
    for x in as_completed(futures):
        if x.result()[0]:
            service_error_dict.update(x.result()[1])
        service_response_dict.update(x.result()[2])
    app.logger.info("--------> Threads Done!")
    return service_error_dict, service_response_dict
