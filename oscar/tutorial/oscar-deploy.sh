kubectl apply -f https://raw.githubusercontent.com/grycap/oscar/master/deploy/yaml/oscar-namespaces.yaml

helm repo add grycap https://grycap.github.io/helm-charts/
helm install --namespace=oscar oscar grycap/oscar --set authPass=oscarPassword --set service.type=ClusterIP --set ingress.create=true --set volume.storageClassName=nfs --set minIO.endpoint=http://minio.minio:9000 --set minIO.TLSVerify=false --set minIO.accessKey=minio --set minIO.secretKey=<MINIO_PASSWORD>
