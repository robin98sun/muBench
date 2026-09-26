import copy
import json
import os
import yaml
from pprint import pprint


K8s_YAML_BUILDER_PATH = os.path.dirname(os.path.abspath(__file__))

SIDECAR_TEMPLATE = "- name: %s-sidecar\n          image: %s"
NODE_AFFINITY_TEMPLATE = {'affinity': {'nodeAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': {'nodeSelectorTerms': [{'matchExpressions': [{'key': 'kubernetes.io/hostname','operator': 'In','values': ['']}]}]}}}}
POD_ANTIAFFINITI_TEMPLATE = {'affinity':{'podAntiAffinity':{'requiredDuringSchedulingIgnoredDuringExecution':[{'labelSelector':{'matchExpressions':[{'key':'app','operator':'In','values':['']}]},'topologyKey':'kubernetes.io/hostname'}]}}}


# PER-SERVICE KEYS WIN, BUT ONLY WHERE A SERVICE SETS THEM. Query workers
# (Robin, 2026-09-25) add query-<type> services that carry their own `replicas`
# (one per query-scheduler node) and their own empty `host-files` (qs nodes
# have no cpu_profiling_result.json). Before this, the global `ms-replica` /
# `replicas` and the global `host-files` always won, and
# customization_work_model / populate_services_with_global_params wrote the
# global values INTO each service, so by the time a deployment was rendered a
# service's own value could no longer be told apart from the global one.
# Recording which of these keys each service carried in the work model FILE,
# before anything overwrites it, is what lets the service's own value win
# while every service that never set the key keeps exactly today's result.
_PER_SERVICE_KEYS = ("replicas", "host-files")
_services_own_keys = {}


def _remember_own_keys(workmodel):
    for service in workmodel:
        if service not in _services_own_keys:
            _services_own_keys[service] = {k for k in _PER_SERVICE_KEYS if k in workmodel[service]}


def _has_own(service, key):
    return key in _services_own_keys.get(service, set())


def _normalize_bool_string(value, default_value=False):
    if value is None:
        value = default_value
    if isinstance(value, bool):
        return "true" if value else "false"

    normalized_value = str(value).strip().lower()
    if normalized_value in {"1", "true", "yes", "on"}:
        return "true"
    if normalized_value in {"0", "false", "no", "off"}:
        return "false"
    return "true" if default_value else "false"


# Override work_model params with those in k8s_parameters
def customization_work_model(workmodel, k8s_parameters):
    _remember_own_keys(workmodel)
    for service in workmodel:
        workmodel[service].update({"url": f"{service}.{k8s_parameters['namespace']}.svc.{k8s_parameters['cluster_domain']}"})
        workmodel[service].update({"path": k8s_parameters['path']})
        workmodel[service].update({"image": k8s_parameters['image']})
        workmodel[service].update({"namespace": k8s_parameters['namespace']})
                    
        if "scheduler-name" in workmodel[service].keys():
            # override scheduler-name value of workmodel.json
            workmodel[service].update({"scheduler-name": k8s_parameters['scheduler-name']})
        if "replicas" in k8s_parameters.keys() and not _has_own(service, "replicas"):
            # override replica value of workmodel.json -- unless the service
            # set its own (query-<type>: one replica per query-scheduler node)
            workmodel[service].update({"replicas": k8s_parameters['replicas']})
        if "cpu-requests" in k8s_parameters.keys():
            # override cpu-requests value of workmodel.json
            workmodel[service].update({"cpu-requests": k8s_parameters['cpu-requests']})
        if "cpu-limits" in k8s_parameters.keys():
            # override cpu-limits value of workmodel.json
            workmodel[service].update({"cpu-limits": k8s_parameters['cpu-limits']})
        if "memory-requests" in k8s_parameters.keys():
            # override memory-requests value of workmodel.json
            workmodel[service].update({"memory-requests": k8s_parameters['memory-requests']})
        if "memory-limits" in k8s_parameters.keys():
            # override memory-limits value of workmodel.json
            workmodel[service].update({"memory-limits": k8s_parameters['memory-limits']})
    print("Work Model Updated!")


def create_deployment_service_yaml_files(workmodel, k8s_parameters, nfs, output_path):
    namespace = k8s_parameters['namespace']
    _remember_own_keys(workmodel)
    counter=0
    logger_level = None
    if "logger_level" in k8s_parameters.keys():
        logger_level = k8s_parameters['logger_level']
    
    for service in workmodel:
        counter=counter+1

        # Create Deployment yamls

        with open(f"{K8s_YAML_BUILDER_PATH}/Templates/DeploymentTemplate.yaml", "r") as file:
            f = file.read()
            f = f.replace("{{SERVICE_NAME}}", service)
            f = f.replace("{{IMAGE}}", workmodel[service]["image"])
            f = f.replace("{{NAMESPACE}}", namespace)
            if "scheduler-name" in workmodel[service].keys():
                f = f.replace("{{SCHEDULER_NAME}}", str(workmodel[service]["scheduler-name"]))
            else:
                f = f.replace("{{SCHEDULER_NAME}}", "default-scheduler")
            if "sidecar" in workmodel[service].keys():
                f = f.replace("{{SIDECAR}}", SIDECAR_TEMPLATE % (service, workmodel[service]["sidecar"]))
            else:
                f = f.replace("{{SIDECAR}}", "".rstrip())
            
            if _has_own(service, "replicas"):
                # the service's own count beats the global ms-replica
                f = f.replace("{{REPLICAS}}", str(workmodel[service]["replicas"]))
            elif "ms-replica" in k8s_parameters.keys():
                f = f.replace("{{REPLICAS}}", str(k8s_parameters["ms-replica"]))
            elif "replicas" in workmodel[service].keys():
                f = f.replace("{{REPLICAS}}", str(workmodel[service]["replicas"]))
            else:
                f = f.replace("{{REPLICAS}}", "1")
            
            if "qos-group" in workmodel[service].keys():
                f = f.replace("{{QOS_GROUP}}", workmodel[service]["qos-group"])
            else:
                f = f.replace("{{QOS_GROUP}}", "be")

            if "cosched-scheduler-name" in k8s_parameters.keys():
                f = f.replace("{{SCHEDULER_IN_COSCHED}}", k8s_parameters["cosched-scheduler-name"])
            else:
                f = f.replace("{{SCHEDULER_IN_COSCHED}}", "none")
            
            if "envoy-concurrency" in k8s_parameters.keys():
                f = f.replace("{{ENVOY_CONCURRENCY}}", str(k8s_parameters["envoy-concurrency"]))
            else:
                f = f.replace("{{ENVOY_CONCURRENCY}}", "2")
            
            # Macaw sub-scheduler knobs. Defaults match the module's own
            # defaults, so a testbed that says nothing about macaw gets a
            # template identical in effect to one without these annotations.
            if "macaw-queue-policy" in k8s_parameters.keys():
                f = f.replace("{{MACAW_QUEUE_POLICY}}", str(k8s_parameters["macaw-queue-policy"]))
            else:
                f = f.replace("{{MACAW_QUEUE_POLICY}}", "edf")

            if "macaw-safety-tick-ms" in k8s_parameters.keys():
                f = f.replace("{{MACAW_SAFETY_TICK_MS}}", str(k8s_parameters["macaw-safety-tick-ms"]))
            else:
                f = f.replace("{{MACAW_SAFETY_TICK_MS}}", "20")

            # THE SYNC-STAT PERIOD IS ALSO THE DEADLINE HORIZON. The sidecar
            # computes it as sync_stat_period_ms x deadline_lookahead_sync_intervals
            # (macaw_filter.c:1543-1547), and that horizon is what answers "is an
            # LS deadline approaching?" for the enforcing-deadline veto. Until now
            # NOTHING could vary it: it was the module's compiled 100 ms default,
            # so the horizon was a constant that no plan could calibrate against
            # its own decision interval. Default stays 100 so an unset plan is
            # byte-identical in effect.
            if "macaw-sync-stat-period-ms" in k8s_parameters.keys():
                f = f.replace("{{MACAW_SYNC_STAT_PERIOD_MS}}", str(k8s_parameters["macaw-sync-stat-period-ms"]))
            else:
                f = f.replace("{{MACAW_SYNC_STAT_PERIOD_MS}}", "100")

            # COSCHED's UDS listener count, not Envoy's worker count (R38 N7).
            # The sidecar rotates its sync-stat callout over
            # cosched-query-scheduler-uds<i>, and those clusters exist for
            # i < cosched-concurrency. Rotating over envoy-concurrency instead
            # (16 on a d430 with `envoy-concurrency: 0`) named a cluster that
            # does not exist in 14 of every 16 rounds. Defaults to 1 because
            # uds0 is the only one guaranteed to be there.
            if "cosched-concurrency" in k8s_parameters.keys():
                f = f.replace("{{COSCHED_CONCURRENCY}}", str(k8s_parameters["cosched-concurrency"]))
            else:
                f = f.replace("{{COSCHED_CONCURRENCY}}", "1")

            # Whether a RESUMED request counts toward deadline pressure
            # (request_scheduler.rs:110): under enforcing-deadline every resume
            # does, under no-later-than-deadline only one accepted while its
            # class was overloaded. The sidecar had no way to know which, so the
            # urgent-on-release half of the pressure was decided by a compiled
            # default.
            #
            # NOTE the plans' top-level `cosched-scheduling-algorithm` is
            # no-later-than-deadline and the Macaw run segment overrides it to
            # enforcing-deadline. Reading the top-level line and calling it
            # "what the plan sets" is how the default came to be wrong; taking
            # it from the resolved parameter is what makes that impossible.
            if "cosched-scheduling-algorithm" in k8s_parameters.keys():
                f = f.replace("{{COSCHED_SCHEDULING_ALGORITHM}}",
                              str(k8s_parameters["cosched-scheduling-algorithm"]))
            else:
                f = f.replace("{{COSCHED_SCHEDULING_ALGORITHM}}", "no-later-than-deadline")

            # The REFERENCE's tick, which sets its admission rate: one resume
            # per worker per tick. The sidecar's own timer is
            # macaw-safety-tick-ms; this is the rate it must reproduce.
            if "tick-period-ms" in k8s_parameters.keys():
                f = f.replace("{{TICK_PERIOD_MS}}", str(k8s_parameters["tick-period-ms"]))
            else:
                f = f.replace("{{TICK_PERIOD_MS}}", "1")

            # A service that places itself -- its own `replicas` AND its own
            # `node_affinity`, i.e. a query-<type> pool -- keeps its node list
            # even when a global worker-node-affinity is set. Every service
            # without its own `replicas` (all the node<i>-* services) takes the
            # branch it took before.
            places_itself = _has_own(service, "replicas") and "node_affinity" in workmodel[service].keys()
            affinity_dict = None
            if "worker-node-affinity" in k8s_parameters.keys() and not places_itself:
                NODE_AFFINITY_TEMPLATE_TO_ADD = NODE_AFFINITY_TEMPLATE.copy()
                # Ensure values is always a list
                worker_affinity_values = k8s_parameters["worker-node-affinity"]["values"]
                if isinstance(worker_affinity_values, str):
                    worker_affinity_values = [worker_affinity_values]
                NODE_AFFINITY_TEMPLATE_TO_ADD['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchExpressions'][0].update({"values" : worker_affinity_values})
                NODE_AFFINITY_TEMPLATE_TO_ADD['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchExpressions'][0].update({"key" : k8s_parameters["worker-node-affinity"]["key"]})
                f = f.replace("{{NODE_AFFINITY}}", str(yaml.dump(NODE_AFFINITY_TEMPLATE_TO_ADD)).rstrip().replace('\n','\n      '))
            elif places_itself:
                # deepcopy with the hostname key set explicitly: .copy() above
                # is shallow, so the branch before this one (and the nginx
                # block below) rewrite the SHARED template's key in place.
                NODE_AFFINITY_TEMPLATE_TO_ADD = copy.deepcopy(NODE_AFFINITY_TEMPLATE)
                node_affinity_value = workmodel[service]["node_affinity"]
                if isinstance(node_affinity_value, str):
                    node_affinity_value = [node_affinity_value]
                match_expression = NODE_AFFINITY_TEMPLATE_TO_ADD['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchExpressions'][0]
                match_expression.update({"key": "kubernetes.io/hostname", "values": list(node_affinity_value)})
                affinity_dict = NODE_AFFINITY_TEMPLATE_TO_ADD
            elif "node_affinity" in workmodel[service].keys():
                NODE_AFFINITY_TEMPLATE_TO_ADD = NODE_AFFINITY_TEMPLATE.copy()
                # Ensure values is always a list
                node_affinity_value = workmodel[service]["node_affinity"]
                if isinstance(node_affinity_value, str):
                    node_affinity_value = [node_affinity_value]
                NODE_AFFINITY_TEMPLATE_TO_ADD['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchExpressions'][0].update({"values" : node_affinity_value})
                f = f.replace("{{NODE_AFFINITY}}", str(yaml.dump(NODE_AFFINITY_TEMPLATE_TO_ADD)).rstrip().replace('\n','\n      '))
            else:
                f = f.replace("{{NODE_AFFINITY}}", "")

            wants_antiaffinity = "pod_antiaffinity" in workmodel[service].keys() and workmodel[service]['pod_antiaffinity']==True
            if affinity_dict is not None:
                # ONE `affinity:` KEY. {{NODE_AFFINITY}} and {{POD_ANTIAFFINITY}}
                # each render their own top-level `affinity:` under the pod
                # spec; with both set the pod spec carries the key twice, and
                # the YAML loader keeps only the LAST -- the anti-affinity --
                # so the node list is dropped without an error and the query
                # pods may land on any node. Merged here into one mapping, and
                # {{POD_ANTIAFFINITY}} left empty.
                if wants_antiaffinity:
                    anti = copy.deepcopy(POD_ANTIAFFINITI_TEMPLATE)
                    anti['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution'][0]['labelSelector']['matchExpressions'][0]['values'][0] = service
                    affinity_dict['affinity']['podAntiAffinity'] = anti['affinity']['podAntiAffinity']
                    wants_antiaffinity = False
                f = f.replace("{{NODE_AFFINITY}}", str(yaml.dump(affinity_dict)).rstrip().replace('\n','\n      '))

            if wants_antiaffinity:
                POD_ANTIAFFINITY_TO_ADD = POD_ANTIAFFINITI_TEMPLATE.copy()
                POD_ANTIAFFINITY_TO_ADD['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution'][0]['labelSelector']['matchExpressions'][0]['values'][0] = service
                POD_ANTIAFFINITY_TO_ADD = str(yaml.dump(POD_ANTIAFFINITY_TO_ADD)).replace('\n','\n        ').rstrip()
                f = f.replace("{{POD_ANTIAFFINITY}}", POD_ANTIAFFINITY_TO_ADD)
            else:
                f = f.replace("{{POD_ANTIAFFINITY}}", "".rstrip())
            if "workers" in workmodel[service].keys():
                f = f.replace("{{PN}}", f'\'{workmodel[service]["workers"]}\'')
            else:
                f = f.replace("{{PN}}", "\'1\'") 
            if "threads" in workmodel[service].keys():
                f = f.replace("{{TN}}", f'\'{workmodel[service]["threads"]}\'')
            else:
                f = f.replace("{{TN}}", "\'4\'")
            
            if logger_level is not None: 
                f = f.replace("{{LOGGER_LEVEL}}", f'\'{logger_level}\'')
            elif "logger_level" in workmodel[service].keys():
                f = f.replace("{{LOGGER_LEVEL}}", f'\'{workmodel[service]["logger_level"]}\'')
            else:
                f = f.replace("{{LOGGER_LEVEL}}", "\'ERROR\'")

            f = f.replace("{{SERVICE_POOL_CONNECTIONS}}", f'\'{k8s_parameters.get("service-pool-connections", 128)}\'')
            f = f.replace("{{SERVICE_POOL_MAXSIZE}}", f'\'{k8s_parameters.get("service-pool-maxsize", 16)}\'')
            service_pool_block = _normalize_bool_string(k8s_parameters.get("service-pool-block"), False)
            f = f.replace("{{SERVICE_POOL_BLOCK}}", f'\'{service_pool_block}\'')
            f = f.replace("{{DNS_NDOTS}}", f'\'{k8s_parameters.get("dns-ndots", 1)}\'')
            
            rank_string='' # ranck string is used to order the yaml file as a funciont of the cpu-requests 
            if  len(set(workmodel[service].keys()).intersection({"cpu-limits","memory-limits","cpu-requests","memory-requests"})):
                s=""
                if "cpu-requests" in workmodel[service].keys() or "memory-requests" in workmodel[service].keys():
                    s = s + "\n            requests:"
                    if "cpu-requests" in workmodel[service].keys():
                        s = s + "\n              cpu: " + workmodel[service]["cpu-requests"]
                        if 'm' in workmodel[service]["cpu-requests"]:
                            rank_string=str(int(workmodel[service]["cpu-requests"].replace('m',''))).zfill(5)
                        else:
                            rank_string=str(int(float(workmodel[service]["cpu-requests"])*1000)).zfill(5)
                    if "memory-requests" in workmodel[service].keys():
                        s = s + "\n              memory: " + workmodel[service]["memory-requests"]
                if "cpu-limits" in workmodel[service].keys() or "memory-limits" in workmodel[service].keys():
                    s = s + "\n            limits:"
                    if "cpu-limits" in workmodel[service].keys():
                        s = s + "\n              cpu: " + workmodel[service]["cpu-limits"]
                    if "memory-limits" in workmodel[service].keys():
                        s = s + "\n              memory: " + workmodel[service]["memory-limits"]
                f = f.replace("{{RESOURCES}}", s)
            else:
                f= f.replace("{{RESOURCES}}", "{}")

            # add host files mount paths and volumes

            host_files_list = []
            if _has_own(service, "host-files"):
                # the service's own list, EVEN WHEN EMPTY, beats the global
                # one: query-<type> pods run on nodes that do not have the
                # global list's cpu_profiling_result.json.
                host_files_list = workmodel[service]["host-files"]
            elif "host-files" in k8s_parameters.keys():
                host_files_list = k8s_parameters["host-files"]

            elif "host-files" in workmodel[service].keys():
                host_files_list = workmodel[service]["host-files"]

            if len(host_files_list) > 0:
                host_files_mount_paths = []
                host_files_volumes = []
                for host_file in host_files_list:
                    if "name" in host_file.keys() and "mount_path" in host_file.keys() and "host_path" in host_file.keys():
                        # Convert underscores to hyphens for Kubernetes compliance
                        volume_name = host_file['name'].replace('_', '-')
                        host_files_mount_paths.append(f"- name: {volume_name}\n              mountPath: {host_file['mount_path']}\n              readOnly: true")
                        host_files_volumes.append(f"- name: {volume_name}\n          hostPath:\n            path: {host_file['host_path']}\n            type: File")
                f = f.replace("{{HOST_FILES_MOUNT_PATHS}}", "\n            ".join(host_files_mount_paths))
                f = f.replace("{{HOST_FILES_VOLUMES}}", "\n        ".join(host_files_volumes))
            else:
                f = f.replace("{{HOST_FILES_MOUNT_PATHS}}", "")
                f = f.replace("{{HOST_FILES_VOLUMES}}", "")

        if not os.path.exists(f"{output_path}/yamls"):
            os.makedirs(f"{output_path}/yamls")
        
        # rank used to sort the deployment so as more demanding PODs are deployed first
        with open(f"{output_path}/yamls/{k8s_parameters['prefix_yaml_file']}-{str(rank_string).zfill(3)}-Deployment-{service}.yaml", "w") as file:
            file.write(f)
        
        # Create Service yamls
        with open(f"{K8s_YAML_BUILDER_PATH}/Templates/ServiceTemplate.yaml", "r") as file:
            f = file.read()
            f = f.replace("{{SERVICE_NAME}}", service)
            f = f.replace("{{NAMESPACE}}", namespace)
        with open(f"{output_path}/yamls/{k8s_parameters['prefix_yaml_file']}-{str(rank_string).zfill(3)}-Service-{service}.yaml", "w") as file:
            file.write(f)

    if k8s_parameters["nginx-gw"] == True:
        # create nginx gw deployment yaml files
        with open(f"{K8s_YAML_BUILDER_PATH}/Templates/ConfigMapNginxGwTemplate.yaml", "r") as file:
            f = file.read()
            f = f.replace("{{NAMESPACE}}", namespace)
            f = f.replace("{{PATH}}", k8s_parameters["path"])
            f = f.replace("{{RESOLVER}}", k8s_parameters["dns-resolver"])

        with open(f"{output_path}/yamls/{k8s_parameters['prefix_yaml_file']}-ConfigMapNginxGw.yaml", "w") as file:
            file.write(f)

        with open(f"{K8s_YAML_BUILDER_PATH}/Templates/DeploymentNginxGwTemplate.yaml", "r") as file:
            f = file.read()
            f = f.replace("{{NAMESPACE}}", namespace)
            f = f.replace("{{SVCTYPE}}", k8s_parameters["nginx-svc-type"])
            f = f.replace("{{NGINX_IMAGE}}", k8s_parameters["nginx-image"])

            if "nginx-replicas" in k8s_parameters.keys():
                f = f.replace("{{NGINX_REPLICAS}}", str(k8s_parameters["nginx-replicas"]))
            else:
                f = f.replace("{{NGINX_REPLICAS}}", "1")

            if "scheduler-name" in workmodel[service].keys():
                f = f.replace("{{SCHEDULER_NAME}}", str(workmodel[service]["scheduler-name"]))
            else:
                f = f.replace("{{SCHEDULER_NAME}}", "default-scheduler")

            if "nginx-node-affinity" in k8s_parameters.keys():
                NGINX_NODE_AFFINITY_TEMPLATE_TO_ADD = NODE_AFFINITY_TEMPLATE.copy()
                NGINX_NODE_AFFINITY_TEMPLATE_TO_ADD['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchExpressions'][0].update({"key" : k8s_parameters["nginx-node-affinity"]["key"]})
                NGINX_NODE_AFFINITY_TEMPLATE_TO_ADD['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchExpressions'][0].update({"values" : k8s_parameters["nginx-node-affinity"]["values"]})
                f = f.replace("{{NODE_AFFINITY}}", str(yaml.dump(NGINX_NODE_AFFINITY_TEMPLATE_TO_ADD)).rstrip().replace('\n','\n      '))
            else:
                f = f.replace("{{NODE_AFFINITY}}", "")
            
        with open(f"{output_path}/yamls/{k8s_parameters['prefix_yaml_file']}-DeploymentNginxGw.yaml", "w") as file:
            file.write(f)
    print("Deployments and Services Created!")

def populate_services_with_global_params(workmodel, k8s_parameters):
    interested_params = ["host-files"]
    _remember_own_keys(workmodel)
    for service in workmodel:
        for param in interested_params:
            # a service's own value (even an empty list) is kept; the pod
            # reads this ConfigMap copy, not the Deployment
            if param in k8s_parameters.keys() and not _has_own(service, param):
                workmodel[service][param] = k8s_parameters[param]
    return workmodel

def create_workmodel_configmap_yaml_file(workmodel, k8s_parameters, nfs, output_path):
    namespace = k8s_parameters['namespace']
    with open(f"{K8s_YAML_BUILDER_PATH}/Templates/ConfigMapWorkmodelTemplate.yaml", "r") as file:
        f = file.read()
        f = f.replace("{{NAMESPACE}}", namespace)
        j = json.dumps(populate_services_with_global_params(workmodel, k8s_parameters),indent=2)
        j = '    '.join(j.splitlines(True))
        f = f.replace("{{WORKMODEL}}", j)
    with open(f"{output_path}/yamls/{k8s_parameters['prefix_yaml_file']}-ConfigMapWorkmodel.yaml", "w") as file:
        file.write(f)
    print("Workmodel Configmap Created!")

def create_internalservice_configmap_yaml_file(k8s_parameters, nfs, output_path, internal_service_functions_path):
    namespace = k8s_parameters['namespace']
    data_dict = dict()
    if internal_service_functions_path != "" or internal_service_functions_path is None:
        src_files = os.listdir(internal_service_functions_path)
        for file_name in src_files:
            full_file_name = os.path.join(internal_service_functions_path, file_name)
            if os.path.isfile(full_file_name):
                with open(full_file_name, 'r') as f:
                    file_content=f.read()
                    data_dict[file_name]=file_content      
    with open(f"{K8s_YAML_BUILDER_PATH}/Templates/ConfigMapInternalServicesTemplate.yaml", "r") as file:
        f = file.read()
        f = f.replace("{{NAMESPACE}}", namespace)
        j = json.dumps(data_dict,indent=2)
        j = '  '.join(j.splitlines(True))
        f = f.replace("{{DATA}}", j)
    with open(f"{output_path}/yamls/{k8s_parameters['prefix_yaml_file']}-ConfigMapInternalServices.yaml", "w") as file:
        file.write(f)
    print("Internal-Services Configmap Created!")
