import random
from readline import append_history_file
import requests
from concurrent.futures import ThreadPoolExecutor, wait, as_completed, FIRST_COMPLETED
import time
import grpc
import mub_pb2_grpc as pb2_grpc
import mub_pb2 as pb2
import json
from pprint import pprint


service_stub = dict()
s = requests.Session()

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
        if ms_trace_input:
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            headers.update(jaeger_context)
            if request_headers:
                for key, value in request_headers.items():
                    headers[key] = value
            json_payload = json.dumps(ms_trace_input)
            app.logger.debug(f'Requesting external service via REST: {service}, headers: {headers}')
            return s.post(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}',data=json_payload,headers=headers)

        elif len(trace)==0 and len(query_string)==0:
            # default 
            return s.get(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}', headers=jaeger_context)
        elif len(trace)>0:
            # trace-driven request
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            headers.update(jaeger_context)
            json_dict = dict()
            json_dict[service] = trace[id][service]
            json_payload = json.dumps(json_dict)
            if  len(query_string)==0:
                return s.post(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}',data=json_payload,headers=headers)
            else:
                return s.post(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}?{query_string}',data=json_payload,headers=headers)
        elif  len(query_string)>0:
            # request with enclosed behaviour information
            return s.get(f'http://{work_model[service_no_escape]["url"]}{work_model[service_no_escape]["path"]}?{query_string}', headers=jaeger_context)  
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
def request_external_service_ms_trace(service_series, id, work_model, s, trace, query_string, app, trace_context, extra_headers=None):
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
            r = request_function(service_name,id,work_model,s,trace,query_string, app, trace_context, ms_trace_input=service_input, request_headers=extra_headers)
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
def run_external_service_ms_trace(external_services, work_model, query_string, trace, app, trace_context=None, request_headers=None):
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
    extra_headers["cosched-fanout"] = fanout
    for service_series in external_services:
        copy_extra_headers = extra_headers.copy()
        copy_extra_headers["cosched-task-index"] = id
        app.logger.debug(f'sending external service request for query {extra_headers["cosched-query-id"]} which fanout is {fanout} and task index is {id}')
        futures.append(pool.submit(request_external_service_ms_trace, service_series, id, work_model, s, trace, query_string, app, trace_context, copy_extra_headers))
        id = id + 1
    wait(futures)
    for x in as_completed(futures):
        if x.result()[0]:
            service_error_dict.update(x.result()[1])
        service_response_dict.update(x.result()[2])
    app.logger.info("--------> Threads Done!")
    return service_error_dict, service_response_dict

