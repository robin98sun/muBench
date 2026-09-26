from kubernetes import client, config, utils
from kubernetes.client.rest import ApiException
import yaml
import json
import os
import time


# How long one Deployment may take to become ready before the deploy stops.
# The wait below had no limit: a pod that can never start (a hostPath File
# missing on its node, a node list no node matches -- the new query-<type>
# pools on the query-scheduler nodes are the case in hand) kept the deploy
# printing "Waiting deployment ... ready" for ever. 900 s by default; the
# environment variable MUBENCH_DEPLOY_READY_TIMEOUT_S overrides it.
DEFAULT_READY_TIMEOUT_S = 900


def _ready_timeout_s(timeout_s):
    if timeout_s is None:
        timeout_s = os.environ.get("MUBENCH_DEPLOY_READY_TIMEOUT_S", DEFAULT_READY_TIMEOUT_S)
    timeout_s = float(timeout_s)
    if timeout_s <= 0:
        raise ValueError(f"deployment ready timeout must be positive, got {timeout_s}")
    return timeout_s


def deploy_items(folder,st,timeout_s=None):
    timeout_s = _ready_timeout_s(timeout_s)
    print("######################")
    print(f"We are going to DEPLOY the yaml files in the following folder: {folder}")
    print("######################")
    config.load_kube_config()
    k8s_apps_api = client.AppsV1Api()
    k8s_core_api = client.CoreV1Api()
    items = list()
    for r, d, f in os.walk(folder):
        f.sort(reverse=True)    # userd to deploy first pods demanding more resources
        for file in f:
            if '.yaml' in file:
                items.append(os.path.join(r, file))

    if os.path.isfile(folder):
        items.append(folder)

    for yaml_to_apply in items:
        with open(yaml_to_apply) as f:
            complete_yaml = yaml.load_all(f,Loader=yaml.FullLoader)
            for partial_yaml in complete_yaml:
                try:
                    if partial_yaml["kind"] == "Deployment":
                        k8s_apps_api.create_namespaced_deployment(namespace=partial_yaml["metadata"]["namespace"], body=partial_yaml)
                        dn=partial_yaml['metadata']['name']
                        api_response = k8s_apps_api.read_namespaced_deployment_status(name=partial_yaml['metadata']['name'], namespace=partial_yaml["metadata"]["namespace"], pretty=True)
                        wait_started = time.time()
                        while (api_response.status.ready_replicas != api_response.status.replicas):
                            waited = time.time() - wait_started
                            if waited > timeout_s:
                                # RuntimeError, not ApiException: the handler
                                # below only prints ApiException and carries
                                # on, and this must stop the deploy.
                                raise RuntimeError(
                                    f"deployment {dn} not ready after {waited:.0f} s "
                                    f"(limit {timeout_s:.0f} s): ready_replicas="
                                    f"{api_response.status.ready_replicas} of replicas="
                                    f"{api_response.status.replicas}; see kubectl describe deployment {dn}")
                            print(f"\n *** Waiting deployment {dn} ready ...*** \n")
                            time.sleep(1)
                            api_response = k8s_apps_api.read_namespaced_deployment_status(name=partial_yaml['metadata']['name'], namespace=partial_yaml["metadata"]["namespace"], pretty=True)
                        time.sleep(st) # used to avoid API server overload
                        print(f"Deployment {dn} created.")
                    elif partial_yaml["kind"] == "Service":
                        k8s_core_api.create_namespaced_service(namespace=partial_yaml["metadata"]["namespace"], body=partial_yaml)
                        print(f"Service '{partial_yaml['metadata']['name']}' created.")
                        print("---")
                    elif partial_yaml["kind"] == "ConfigMap":
                            k8s_core_api.create_namespaced_config_map(namespace=partial_yaml["metadata"]["namespace"], body=partial_yaml)
                            print(f"ConfigMap '{partial_yaml['metadata']['name']}' created.")
                            print("---")
                except ApiException as err:
                    api_exception_body = json.loads(err.body)
                    print("######################")
                    print(f"Exception raised deploying a yaml file: {yaml_to_apply}")
                    if "details" in api_exception_body.keys() and "reason" in api_exception_body.keys():
                        print(f"Exception raised deploying a {partial_yaml['kind']}: {api_exception_body['details']} -> {api_exception_body['reason']}")
                    else:
                        print(f"Exception raised deploying a {partial_yaml['kind']}: {api_exception_body}")
                    print("######################")                

def undeploy_items(folder):
    print("######################")
    print(f"We are going to UNDEPLOY the yaml files in the following folder: {folder}")
    print("######################")
    config.load_kube_config()
    k8s_apps_api = client.AppsV1Api()
    k8s_core_api = client.CoreV1Api()
    items = list()
    for r, d, f in os.walk(folder):
        for file in f:
            if '.yaml' in file:
                items.append(os.path.join(r, file))
    if os.path.isfile(folder):
        items.append(folder)
    for yaml_to_create in items:
        with open(yaml_to_create) as f:
            complete_yaml = yaml.load_all(f,Loader=yaml.FullLoader)
            for partial_yaml in complete_yaml:
                try:
                    if partial_yaml["kind"] == "Deployment":
                        dep_name = partial_yaml["metadata"]["name"]
                        resp = k8s_apps_api.delete_namespaced_deployment(name=dep_name, namespace=partial_yaml["metadata"]["namespace"], grace_period_seconds=0)
                        print(f"Deployment '{dep_name}' deleted.")
                    elif partial_yaml["kind"] == "Service":
                        svc_name = partial_yaml["metadata"]["name"]
                        resp = k8s_core_api.delete_namespaced_service(name=svc_name, namespace=partial_yaml["metadata"]["namespace"], grace_period_seconds=0)
                        print(f"Service '{svc_name}' deleted.")
                        print("---")
                    elif partial_yaml["kind"] == "ConfigMap":
                        map_name = partial_yaml["metadata"]["name"]
                        resp = k8s_core_api.delete_namespaced_config_map(name=map_name, namespace=partial_yaml["metadata"]["namespace"])
                        print(f"ConfigMap '{map_name}' deleted.")
                        print("---")
                except ApiException as err:
                    api_exception_body = json.loads(err.body)
                    print("######################")
                    print(f"Exception raised trying to delete {partial_yaml['kind']} '{api_exception_body['details']['name']}': {api_exception_body['reason']}")
                    print("######################")
