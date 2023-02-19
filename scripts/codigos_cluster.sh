# --------------------------------------------------
aws configure
# --------------------------------------------------
eksctl create cluster \
--name=gabrielteste \
--managed \
--instance-types=m5.large \
--spot \
--nodes-min=2 \
--nodes-max=4 \
--region=us-east-2 \
--alb-ingress-access \
--node-private-networking \
--full-ecr-access \
--nodegroup-name=ng-gabrielteste \
--color=fabulous
# 
kubectl get nodes ou kubectl get namespaces.
# 
eksctl delete cluster \
--region us-east-2 \
--name gabrielteste \
--color=fabulous
# 
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml
kubectl get namespaces
cd kubernetes 
kubectl apply -f dashboard
kubectl -n kubernetes-dashboard create token admin-user.
kubectl proxy
kubectl delete -f dashboard
kubectl delete -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml
choco install kubernetes-helm
kubectl get namespaces 
kubectl create namespace airflow
kubectl get all -n airflow
helm repo add apache-airflow https://airflow.apache.org
helm repo update
cd kubernetes
helm show values apache-airflow/airflow > airflow/myvalues.yaml
# 
helm install airflow apache-airflow/airflow \
-f airflow/myvalues.yaml \
-n airflow \ 
--debug
# 
kubectl get pods -n airflow
kubectl get svc -n airflow
kubectl create namespace processing
# 
docker build -t gabrielnovais/spark-operator:v3.0.0-aws
docker login -u gabrielnovais -p minhasenha.
docker push gabrielnovais/spark-operator:v3.0.0-aws.
# 
kubectl create serviceaccount spark -n processing
kubectl get clusterroles --all-namespaces
# 
kubectl create clusterrolebinding spark-role-binding \
--clusterrole=edit \
--serviceaccount=processing:spark \
-n processing
kubectl apply -f kubernetes/airflow/rolebinding_for_airflow.yaml -n airflow.
kubectl get serviceaccount -n airflow.
kubectl get clusterrolebinding -n airflow
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator.
helm repo update
helm install spark spark-operator/spark-operator -n processing
helm ls -n processing
kubectl get pods -n processing
kubectl describe secret aws-credentials -n processing
# 
kubectl create secret generic aws-credentials \
--from-literal=aws_access_key_id=meukeyid \
-from-literal=aws_secret_access_key=meusecretkey \
-n processing
#
kubectl apply -f spark-batch-operator-k8s-v1beta2.yaml -n processing. 
kubexctl get pods -n processing
kubectl get sparkapplication -n processing
kubectl get pods -n processing
kubectl get pods -n airflow --watch
kubectl get sparkapplication -n airflow