#!/bin/sh

# Creates a local Oscar cluster with credentials:
#            user  - Password
#     minio: minio - minioPassword
#     oscar: oscar - oscarPassword
# Check tutorial at: https://docs.oscar.grycap.net/local-testing/
#
# Prerequeisites: docker, kubectl, helm and kind.
# To delete the deployed cluster use: kind delete cluster

# Create cluster using kind
echo '##### Creating kind cluster #####'
cat <<EOF | kind create cluster --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  - containerPort: 80
    hostPort: 80
    protocol: TCP
  - containerPort: 443
    hostPort: 443
    protocol: TCP
  - containerPort: 30300
    hostPort: 30300
    protocol: TCP
  - containerPort: 30301
    hostPort: 30301
    protocol: TCP
EOF

# Deploy NGINGX
echo '##### Deploying NGINX #####'
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/master/deploy/static/provider/kind/deploy.yaml

# Deploy MinIO
echo '##### Deploying MINIO #####'
helm repo add minio https://charts.min.io
helm install minio minio/minio --namespace minio --set rootUser=minio,rootPassword=minioPassword,service.type=NodePort,service.nodePort=30300,consoleService.type=NodePort,consoleService.nodePort=30301,mode=standalone,resources.requests.memory=512Mi,environment.MINIO_BROWSER_REDIRECT_URL=http://localhost:30301 --create-namespace

# Deploy NFS Server
echo '##### Deploying NFS Server #####'
helm repo add nfs-ganesha-server-and-external-provisioner https://kubernetes-sigs.github.io/nfs-ganesha-server-and-external-provisioner/
helm install nfs-server-provisioner nfs-ganesha-server-and-external-provisioner/nfs-server-provisioner

# Deploy knative for serverless backend
echo '##### Deploying KNATIVE #####'
kubectl apply -f https://github.com/knative/operator/releases/download/knative-v1.3.1/operator.yaml

cat <<EOF | kubectl apply -f -
---
apiVersion: v1
kind: Namespace
metadata:
  name: knative-serving
---
apiVersion: operator.knative.dev/v1beta1
kind: KnativeServing
metadata:
  name: knative-serving
  namespace: knative-serving
spec:
  version: 1.3.0
  ingress:
    kourier:
      enabled: true
      service-type: ClusterIP
  config:
    config-features:
      kubernetes.podspec-persistent-volume-claim: enabled
      kubernetes.podspec-persistent-volume-write: enabled
    network:
      ingress-class: "kourier.ingress.networking.knative.dev"
EOF


# Deploy OSCAR
echo 'Sleeping for some seconds before deploying OSCAR'
sleep 10
echo '##### Deploying OSCAR #####'
kubectl apply -f https://raw.githubusercontent.com/grycap/oscar/master/deploy/yaml/oscar-namespaces.yaml

helm repo add grycap https://grycap.github.io/helm-charts/
helm install --namespace=oscar oscar grycap/oscar --set authPass=oscarPassword --set service.type=ClusterIP --set ingress.create=true --set volume.storageClassName=nfs --set minIO.endpoint=http://minio.minio:9000 --set minIO.TLSVerify=false --set minIO.accessKey=minio --set minIO.secretKey=minioPassword
