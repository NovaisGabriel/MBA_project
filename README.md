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
-> Linha 987: Trocar type por LoadBalancer. Sem isso, só conseguiremos acessar o link do webserver para ver a UI do airflow através de configurações manuais.

-> Linha 1507: Na parte de redis utilizaremos false.
-> Linha 1782: Utilizar o git sync, fazendo ficar true.

Antes de fazer mais modificações dar um push com branch dev no git pra salvar e preparar para quando futuras modificações ocorrerem o airflow identificar o evento. Entrar na branch dev do github e coletar o link https do repositório.

-> Linha 1788: colar link do repositório.
-> Linha 1789: trocar por branch dev.
-> Linha 1796: trocar pelas nossas dags, com o path "dags". 

Feitas as modificações faremos o deploy. Vamos rodar o seguinte código: helm install airflow apache-airflow/airflow -f airflow/myvalues.yaml -n airflow --debug

Caso tenha dado algum erro basta desinstalar com o código: helm uninstall airflow -n airflow. Naõ esquecer de deletar os load balancers também, com o código: kubectl delete svc airflow-websever -n airflow. Deletar também o postgres, com o código: kubectl delete pvc data-airflow-postgresql-0 -n airflow.

Verificaremos se está tudo certo: kubectl get pods -n airflow
Para pegar o link do serviço do airflow rodar kubectl get svc -n airflow. Estará na coluna External-IP. Não esquecer de adicionar a porta :8080. Costuma demorar uns minutinhos até ficar disponível.

No Login do airflow utilizar as credenciais que foram definidas no arquivo myvalues.yaml. Após entrar no airflow devemos modificar modificar a senha de entrada no serviço, pois a senha de início é fraca "admin". Refazer a autenticação com nova senha.

Entrar em Admin, depois Connections e adicionar no botão de mais (+). Escolher no campo connection id um nome, como "my_aws". Escolher no tipo, Amazon web services. Adicionar as credenciais da AWS. Estará tudo pronto para triggar as suas DAGs.

12) Deploy do Spark:

Primeiro devemos criar um namespace rodando: kubectl create namespace processing
Criaremos dentro da subpasta criada em kubernetes com nome spark, um outra pasta chamada jars e um dockerfile fora dela. Muito cuidado com as versões das libs, em especial quando se trata de spark, pois isso é muito crítico.

Com isso vamos buildar o dockerfile. Rode o comando: "docker build -t gabrielnovais/spark-operator:v3.0.0-aws .". Esteja com o docker desktop aberto para verificar que localmente a imagem será criada e é interessante que já esteja logado por lá. É preciso fazer o login da sua conta no docker. Realize o login com o código: docker login -u gabrielnovais -p minhasenha. Vamos fazer o push da imagem para ficar disponível publicamente, com o código a seguir: docker push gabrielnovais/spark-operator:v3.0.0-aws.

Feito o push pode verificar no docker hub que a imagem estará lá. Agora o que precisaremos será coletar as funções jars no maven. OS links de download dos 4 arquivos jars são:

Em https://mvnrepository.com/:

-> aws-java-sdk: https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
-> hadoop: https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar
-> Delta: https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.0.0/delta-core_2.12-1.0.0.jar
-> spark-sql-kafka: https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.1/spark-sql-kafka-0-10_2.12-3.0.1.jar

Precisamos então criar um service account no kubernetes spark. Para isso rode o código: kubectl create serviceaccount spark -n processing

Para entender as roles devemos verificar quais são as disponíveis. Para isso rode: kubectl get clusterroles --all-namespaces. Vamos criar uma role, para isso rode: kubectl create clusterrolebinding spark-role-binding --clusterrole=edit --serviceaccount=processing:spark -n processing

Precisamos criar um operator (responsável por criar novos recursos no kubernetes) para utilizar o spark application no kubernetes. Para fazer isso precisamos antes utilizar o helm: helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator. Depois de um update no helm com helm repo update. Então poderemos fazer a instalação do spark operator com o código: helm install spark spark-operator/spark-operator -n processing. Para verificar se o spark operator for deployado então é só fazer helm ls -n processing. E depois de uma olhada no kubectl get pods -n processing

