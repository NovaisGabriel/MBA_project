# project

Project started in January 08, 2023.


etapas para construção do projeto:

0) Instalar chocolately (windows) para realizar algumas etapas.
1) Criar repositório no github
2) Instalar o Rony. Link: https://github.com/A3Data/rony
3) Utilizar o Rony para construir as principais pastas e arquivos: rony new "nome do projeto"
4) Instalar o eksctl command line utility. Link: https://docs.aws.amazon.com/eks/latest/userguide/eksctl.html

5) Configurar chaves de acesso e região com o CLI da AWS: aws configure

6) Criar um cluster kubernetes na aws com o comando: eksctl create cluster --name=gabrielteste --managed --instance-types=m5.large --spot --nodes-min=2 --nodes-max=4 --region=us-east-2 --alb-ingress-access --node-private-networking --full-ecr-access --nodegroup-name=ng-gabrielteste --color=fabulous
obs: o eksctl roda utilzando o AWS CloudFormation que é "um serviço que fornece aos desenvolvedores e empresas uma forma fácil de criar um conjunto de recursos relacionados da AWS e de terceiros para provisioná-los e gerenciá-los de forma organizada e previsível". Para verificar a criação do cluster entre na AWS dentro de CloudFormation e verifique dentro da região selecionada. É possivel olhar também dentro do EKS na AWS.

7) Para confirmar a criação do cluster, digite os comandos: kubectl get nodes ou kubectl get namespaces.

8) Quando for necessário deletar o cluster basta rodar o código: eksctl delete cluster --region us-east-2 --name gabrielteste --color=fabulous

9) Se fosse o caso de termos mais de um cluster kubernetes em outras clouds, seria interessante instalar o kubectx para realizar o manager desses cluster.

OBS: alguns comandos básicos do kubectl:

-> kubectl get nodes
-> kubectl get namespaces
-> kubectl get pods ou kubectl get pods -n kube-node-lease ou kubectl get pods -n kube-system
-> kubectl get svc -n kube-system
-> kubectl get deployments -n kube-system
-> kubectl get rs -n kube-system
-> kubectl get pv -n kube-system ou kubectl get pvc -n kube-system

10) Para deployar o dashboard do kubernetes, visitar o link: https://github.com/kubernetes/dashboard.

Devemos rodar o seguinte código: kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml

Para verificar se o dashboard foi deployado rodar: kubectl get namespaces

Criar pasta kubernetes no diretório principal.
Criar uma subpasta dashboard na pasta kubernetes
Criar arquivo yaml com nome serviceaccount.
Criar outro arquivo chamado rolebind.yaml (conteúdos estão dentro da pasta de config)

Entrar no diretório e deployar os recursos: cd kubernetes e depois kubectl apply -f dashboard

Criar secrets keys com o código: kubectl -n kubernetes-dashboard create token admin-user. Copiar chave.

Rodar o código para startar o server: kubectl proxy

Link para o dashboard: http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/

Escolher token, e colar a chave criada no campo destacado logo quando entrar no dashboard.

Para deletar os recursos criados basta rodar os códigos: kubectl delete -f dashboard. E para deletar o dashboard basta fazer kubectl delete -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml

11) Deploy do airflow:
Instalar o helm localmente, visitar o link: https://helm.sh/docs/intro/install/
Abrir o powershell com permissão de administrador e rodar o código:
choco install kubernetes-helm
Já temos um cluster kubernetes rodando.
Criar pasta airflow dentro de kubernetes.
Utilizaremos a ferramenta Helm Chart, verificar link: https://airflow.apache.org/docs/helm-chart/stable/index.html
Rodar kubectl get namespaces para verificar se já existe algum namespace do airflow. 
Caso não exista rodar: kubectl create namespace airflow
Verificar os serviços existentes dentro do namespace airflow com o código: kubectl get all -n airflow

Vamo adicionar o helm chart com o seguinte comando: helm repo add apache-airflow https://airflow.apache.org depois helm repo update só para confirmar.

Entrar na pasta kubernetes (cd kubernetes).
Rodar o código para adicionar arquivo yaml oficial do apache airflow: helm show values apache-airflow/airflow > airflow/myvalues.yaml

Criar na root do projeto uma nova pasta de dags. Para estudar e realizar testes é possível criar uma task de exemplo com o link: https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html

Vamos modificar apenas alguns pontos do arquivo yaml do airflow:
-> Linha 233: Modificar para KubernetesExecutor
-> linha 242: Modificar para termos logs. É preciso um bucket do S3 na mesma região.
    - name: AIRFLOW__CORE__REMOTE_LOGGING
      value:'True'
    - name: AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER
      value: "s3://testekuberneteslogs/airflow-logs/"
    - name: AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID
      value: "my_aws"

-> Linha 379: Para adicionar o fernetKey, precisamos primeiro criar um arquivo dentro de airflow para gerar esta chave, através do fernet. O arquivo python, após rodar gerará uma chave. O conteúdo do python está em config. Colar o valor da chave no til da linha.

-> Linha 956: Na parte de Default User, trocar para as suas informações.

-> Linha 1507: Na parte de redis utilizaremos false.
-> Linha 1782: Utilizar o git sync, fazendo ficar true.
