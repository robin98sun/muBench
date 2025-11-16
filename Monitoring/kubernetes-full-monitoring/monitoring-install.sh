#! /bin/bash
# Secure .kube
microservice_namespace="ms"
head_node_name="master"
master_ip="master_ip"
docker_registry_server="docker.io"
skip_registry_auth=false
docker_registry_cert=""
node_label_key="" 
node_label_value=""
istio_replica_count=1
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
        --docker-registry=*)
            docker_registry_server="${1#*=}"
            shift
            ;;
        --skip-registry-auth)
            skip_registry_auth=true
            shift
            ;;
        --docker-registry-cert=*)
            docker_registry_cert="${1#*=}"
            shift
            ;;
        --node-label-key=*)
            node_label_key="${1#*=}"
            shift
            ;;
        --node-label-value=*)
            node_label_value="${1#*=}"
            shift
            ;;
        --istio-replica-count=*)
            istio_replica_count="${1#*=}"
            shift
            ;;
        *)
            shift
            ;;
    esac
done

echo "Monitoring installation script"
echo "Usage: $0 [options]"
echo "Options:"
echo "  --ns=<namespace>                    Microservice namespace (default: ms)"
echo "  --head-node-name=<name>            Head node name (default: master)"
echo "  --master-ip=<ip>                   Master IP address (default: master_ip)"
echo "  --docker-registry=<registry>      Docker registry server (default: 44.251.28.34:30500)"
echo "  --skip-registry-auth               Skip creating registry authentication secret"
echo "  --docker-registry-cert=<cert>      Docker registry certificate (default: empty)"
echo ""
echo "Current configuration:"
echo "  Microservice namespace: ${microservice_namespace}"
echo "  Head node name: ${head_node_name}"
echo "  Master IP: ${master_ip}"
echo "  Docker registry: ${docker_registry_server}"
echo "  Skip registry auth: ${skip_registry_auth}"
echo "  Docker registry certificate: ${docker_registry_cert}"
echo ""



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

# Create registry secret for private Docker registry (only if authentication is required)
if [ "$skip_registry_auth" = true ]; then
    echo "Skipping registry secret creation (--skip-registry-auth flag set)"
else
    echo "Creating registry secret for private Docker registry: ${docker_registry_server}"
    kubectl create secret docker-registry registry-secret \
        --docker-server=${docker_registry_server} \
        --docker-username=admin \
        --docker-password=admin123 \
        --docker-email=admin@example.com -n istio-system || echo "Registry secret already exists"
fi

if [ -z "${docker_registry_cert}" ]; then
  echo "No Docker registry certificate provided, using default values"
  echo "----------------------------------------"
  cat <<EOF > istio-values.yaml

global:
  hub: ${docker_registry_server}
  imagePullPolicy: IfNotPresent
EOF
# Only add imagePullSecrets if authentication is not skipped
  if [ "$skip_registry_auth" = false ]; then
    cat <<EOF >> istio-values.yaml
  imagePullSecrets:
    - name: registry-secret
EOF
  fi

  cat <<EOF >> istio-values.yaml

pilot:
  nodeSelector:
    node-role.cosched.io: standby-worker
  # Or full affinity if you want stricter control
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: node-role.cosched.io
            operator: In
            values:
            - standby-worker

# Disable TLS verification for private registry
gateways:
  istio-ingressgateway:
    replicaCount: ${istio_replica_count}
    resources:
      limits:
        cpu: "4"
        memory: "4Gi"
    nodeSelector:
      node-role.cosched.io: standby-worker
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: node-role.cosched.io
              operator: In
              values:
              - standby-worker
EOF
  echo "----------------------------------------"
  echo "istio-values.yaml"
  cat istio-values.yaml
  echo "----------------------------------------"
  echo "helm upgrade --install istio-base istio/base -n istio-system -f istio-values.yaml"
  helm upgrade --install istio-base istio/base -n istio-system -f istio-values.yaml
  echo "----------------------------------------"
  echo "helm upgrade --install istiod istio/istiod -n istio-system --set global.proxy.tracer="zipkin" --wait -f istio-values.yaml"
  echo "----------------------------------------"
  helm upgrade --install istiod istio/istiod -n istio-system --set global.proxy.tracer="zipkin" --wait -f istio-values.yaml

else

  echo "Adding CA certificate to Istio configuration..."
  
  # Read the certificate content
  cert_content=$(cat ${docker_registry_cert})
  
  # Create istio-values with CA certificate
  cat <<EOF > istio-values-with-cert.yaml

global:
  hub: ${docker_registry_server}
  imagePullPolicy: IfNotPresent
EOF

  # Only add imagePullSecrets if authentication is not skipped
  if [ "$skip_registry_auth" = false ]; then
    cat <<EOF >> istio-values-with-cert.yaml
  imagePullSecrets:
    - name: registry-secret
EOF
  fi

  cat <<EOF >> istio-values-with-cert.yaml

pilot:
  nodeSelector:
    node-role.cosched.io: standby-worker
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: node-role.cosched.io
            operator: In
            values:
            - standby-worker

# Add CA certificate to mesh configuration
meshConfig:
  caCertificates:
    - pem: |
$(echo "${cert_content}" | sed 's/^/        /')

gateways:
  istio-ingressgateway:
    replicaCount: ${istio_replica_count}
    resources:
      limits:
        cpu: "4"
        memory: "4Gi"
    nodeSelector:
      node-role.cosched.io: standby-worker
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: node-role.cosched.io
              operator: In
              values:
              - standby-worker
EOF

  echo "----------------------------------------"
  echo "istio-values-with-cert.yaml"
  cat istio-values-with-cert.yaml
  echo "----------------------------------------"
  echo "helm upgrade --install istio-base istio/base -n istio-system -f istio-values-with-cert.yaml"
  helm upgrade --install istio-base istio/base -n istio-system -f istio-values-with-cert.yaml
  echo "----------------------------------------"
  echo "helm upgrade --install istiod istio/istiod -n istio-system --set global.proxy.tracer="zipkin" --wait -f istio-values-with-cert.yaml"
  helm upgrade --install istiod istio/istiod -n istio-system --set global.proxy.tracer="zipkin" --wait -f istio-values-with-cert.yaml
  if [[ $? -ne 0 ]]; then
    echo "ERROR: Failed to install istiod"
    exit 1
  fi
fi

cat <<EOF > istio-gateway-values.yaml
replicaCount: ${istio_replica_count}
resources:
  limits:
    cpu: "4"
    memory: "4Gi"
nodeSelector:
  node-role.cosched.io: standby-worker
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: node-role.cosched.io
          operator: In
          values:
          - standby-worker
EOF

echo "helm upgrade --install istio-ingressgateway istio/gateway -n istio-system -f istio-gateway-values.yaml"
helm upgrade --install istio-ingressgateway istio/gateway -n istio-system -f istio-gateway-values.yaml

echo "kubectl patch deployment istio-ingressgateway -n istio-system --type='merge' -p '{\"spec\":{\"progressDeadlineSeconds\":3600}}'"
kubectl patch deployment istio-ingressgateway -n istio-system --type='merge' -p '{"spec":{"progressDeadlineSeconds":3600}}'
echo "----------------------------------------"
echo "kubectl patch deployment istio-ingressgateway -n istio-system --type='merge' -p '{\"spec\":{\"replicas\":${istio_replica_count}}}'"
kubectl patch deployment istio-ingressgateway -n istio-system --type='merge' -p "{\"spec\":{\"replicas\":${istio_replica_count}}}"
echo "----------------------------------------"
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

echo "helm upgrade --install -n istio-system -f kiali-values.yaml -f kiali-scheduling.yaml kiali-server kiali/kiali-server"
helm upgrade --install \
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

