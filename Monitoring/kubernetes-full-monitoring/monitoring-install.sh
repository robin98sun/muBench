#! /bin/bash
# Secure .kube
microservice_namespace="ms"
head_node_name="master"
master_ip="master_ip"
while [[ $# -gt 0 ]]; do
    case $1 in
        --ns=*)
            microservice_namespace="${1#*=}"
            shift
            ;;
        --head-node-name=*)
            head_node_name="${1#*=}"
            shift
            ;;
        --master-ip=*)
            master_ip="${1#*=}"
            shift
            ;;
        *)
            shift
            ;;
    esac
done



if [[ -z $(kubectl get namespace ${microservice_namespace}) ]]; then
    echo "Namespace ${microservice_namespace} does not exist, creating it"
    kubectl create namespace ${microservice_namespace}
fi

echo "Head node name: ${head_node_name}"

# #########################################################################################################################
# Uninstall if already installed
# #########################################################################################################################
if [[ -n $(helm list -n monitoring) ]]; then
    echo "Uninstalling prometheus, istio-base, istiod, istio-ingressgateway, jaeger, kiali"
    helm uninstall prometheus -n monitoring
    helm uninstall prometheus-operator -n monitoring
    helm uninstall istio-base -n istio-system
    helm uninstall istiod -n istio-system
    helm uninstall kiali -n istio-system
    helm uninstall kiali-server -n istio-system
    echo "Deleting namespace monitoring"
    kubectl delete namespace monitoring
    echo "Deleting namespace istio-system"
    kubectl delete namespace istio-system
    echo "Existing monitoring components deleted"
fi

chmod go-r -R ~/.kube/

# #########################################################################################################################
# Prometheus
# #########################################################################################################################
kubectl create namespace monitoring
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update


cat <<EOF > prometheus-values.yaml
# values.yaml

# Global defaults for affinity
global:
  podAffinityPreset: ""
  podAntiAffinityPreset: ""
  nodeAffinityPreset:
    type: hard
    key: kubernetes.io/hostname
    values:
      - ${head_node_name}

# Or more fine-grained (per component):
prometheus:
  prometheusSpec:
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values:
              - ${head_node_name}

alertmanager:
  alertmanagerSpec:
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values:
              - ${head_node_name}

grafana:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/hostname
            operator: In
            values:
            - ${head_node_name}

prometheusOperator:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/hostname
            operator: In
            values:
            - ${head_node_name}

kube-state-metrics:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/hostname
            operator: In
            values:
            - ${head_node_name}
EOF

echo "helm install prometheus prometheus-community/kube-prometheus-stack -n monitoring -f prometheus-values.yaml"
helm install prometheus prometheus-community/kube-prometheus-stack -n monitoring -f prometheus-values.yaml

# Prometheus (30000) and Grafana (30001) NodePort Services
echo "kubectl apply -f prometheus-nodeport.yaml -n monitoring"
kubectl apply -f prometheus-nodeport.yaml -n monitoring
echo "kubectl apply -f grafana-nodeport.yaml -n monitoring"
kubectl apply -f grafana-nodeport.yaml -n monitoring

# #########################################################################################################################
# Istio
# #########################################################################################################################
helm repo add istio https://istio-release.storage.googleapis.com/charts
helm repo update

echo "kubectl create namespace istio-system"
kubectl create namespace istio-system

cat <<EOF > istio-values.yaml

pilot:
  nodeSelector:
    kubernetes.io/hostname: ${head_node_name}
  # Or full affinity if you want stricter control
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/hostname
            operator: In
            values:
            - ${head_node_name}
EOF

echo "helm install istio-base istio/base -n istio-system -f istio-values.yaml"
helm install istio-base istio/base -n istio-system -f istio-values.yaml

echo "helm install istiod istio/istiod -n istio-system --set global.proxy.tracer="zipkin" --wait -f istio-values.yaml"
helm install istiod istio/istiod -n istio-system --set global.proxy.tracer="zipkin" --wait -f istio-values.yaml

cat <<EOF > istio-gateway-values.yaml
nodeSelector:
  kubernetes.io/hostname: ${head_node_name}
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
          - ${head_node_name}
EOF

echo "helm install istio-ingressgateway istio/gateway -n istio-system -f istio-gateway-values.yaml"
helm install istio-ingressgateway istio/gateway -n istio-system -f istio-gateway-values.yaml

echo "kubectl label namespace ${microservice_namespace} istio-injection=enabled"
kubectl label namespace ${microservice_namespace} istio-injection=enabled

# Istio - Prometeus integration
echo "kubectl apply -f istio-prometheus-operator.yaml"
kubectl apply -f istio-prometheus-operator.yaml

# #########################################################################################################################
# Jaeger
# #########################################################################################################################
echo "sed -i 's/<node-name>/${head_node_name}/g' jaeger.yaml"
sed -i "s/robin-llm-openwhisk-head/${head_node_name}/g" jaeger.yaml
echo "kubectl apply -f jaeger.yaml"
kubectl apply -f jaeger.yaml

# Jaeger NodePort Service (30002)
echo "kubectl apply -f jaeger-nodeport.yaml"
kubectl apply -f jaeger-nodeport.yaml

# #########################################################################################################################
# Kiali
# #########################################################################################################################
helm repo add kiali https://kiali.org/helm-charts
helm repo update

cat <<EOF > kiali-scheduling.yaml
deployment:
  node_selector:
    kubernetes.io/hostname: ${head_node_name}
EOF

echo "helm install -n istio-system -f kiali-values.yaml -f kiali-scheduling.yaml kiali-server kiali/kiali-server"
helm install \
  -n istio-system \
  -f kiali-values.yaml \
  -f kiali-scheduling.yaml \
  kiali-server \
  kiali/kiali-server

#Kiali NodePort Service (30003)
echo "kubectl apply -f kiali-nodeport.yaml"
kubectl apply -f kiali-nodeport.yaml

# #########################################################################################################################
echo "Monitoring installation completed"
echo "Grafana admin user: admin"
echo "Grafana admin password: $(kubectl --namespace monitoring get secrets prometheus-grafana -o jsonpath="{.data.admin-password}" | base64 -d)"
echo "Prometheus URL: http://${master_ip}:30000"
echo "Grafana URL: http://${master_ip}:30001"
echo "Jaeger URL: http://${master_ip}:30002"
echo "Kiali URL: http://${master_ip}:30003"
echo "Nginx API Gateway URL: http://${master_ip}:31113"
echo "----------------------------------------"

