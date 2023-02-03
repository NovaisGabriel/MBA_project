# Projeto Aplicado - Engenharia de Dados - MBA/XPE

O objetivo do projeto é o de proporcionar o tratamento de dados, armazenamento de dados processados e disponibilização de dados para duas finalidades distintas: uma focada nas análises de suporte de tomada de decisão, e outra com a finalidade de melhorar a personalização da experiência do usuário. Objetivou-se fornecer dados tratados e com valor adicionado que possa estar acessível por um DataWarehouse disponível em um serviço de Cloud permitindo extrair análises de duas naturezas distintas. A primeira natureza é um acompanhamento das métricas de desempenho do negócio. E a segunda é disponibilizar para clientes do Varejo E-commerce recomendações personalizadas. O usuário terá acesso a pelo menos uma plataforma para dar entrada a rotinas SQL para extrair informação dos dados. A maior parte da infraestrutura será construída em forma de código e estará em nuvem. O projeto deve ser concluído até Março de 2023.

## Arquitetura

A arquitetura construída para a solução, destaca-se na Figura 12 abaixo. Nela pode-se verificar que os dados serão distribuídos em 3 buckets no S3: um para ingestão, outro para processamento e outro para consumo. Dessa forma a estrutura de processamento formada pelo cluster Kebernetes consegue organizar os inputs e outputs e garante a transferência desses dados de maneira rápida e fácil através do AWS Glue Crawler para o serviço de DataWarehouse da AWS, o Athena, local onde os analistas poderão realizar as consultas necessárias e eventuais diligências de ações para rentabilização. A forma de construção da parte de buckets e serviços do Glue Crawler serão efetivadas por meio de estratégia IaC, conforme ferramental do Terraform.

![arquitetura](imgs\arquitetura.jpg)


## Construção e Deploy

As etapas para construção do projeto e seu deploy na cloud da AWS, de acordo com a arquitetura descrita acima, segue no roteiro abaixo:

### 1. Ferramentas de Ambiente:

Instalar chocolately (windows) para realizar algumas etapas.

Criar repositório no github

Instalar o Rony. Link: https://github.com/A3Data/rony

Utilizar o Rony para construir as principais pastas e arquivos: rony new "nome do projeto"

Instalar o eksctl command line utility. Link: https://docs.aws.amazon.com/eks/latest/userguide/eksctl.html

### 2. Configurar chaves de acesso e região com o CLI da AWS: aws configure

Criar um cluster kubernetes na aws com o comando:

```sh
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
```

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

Não esquecer de adicionar na lista de variáveis dentro do airflow, na parte de Admin as mesmas credenciais com nomes de aws_access_key_id e aws_secret_access_key.

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

Após criar o cluster role acima, pegue o arquivo yaml gerado e coloque na pasta do airflow, com nome: "rolebinding_for_airflow.yaml". Nele além do código que será originado, adicione o seguinte trecho de código para que o airflow tenha permissões:

->  apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRoleBinding
    metadata:
      name: airflow-spark-crb
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: ClusterRole
      name: spark-cluster-cr
    subjects:
      - kind: ServiceAccount
        name: airflow-worker
        namespace: airflow

Após adicionar essas permissões para o airflow, devemos aplicar ao kubernetes com o código: kubectl apply -f kubernetes/airflow/rolebinding_for_airflow.yaml -n airflow. Para conseguir verificar se foi aplicado basta fazer kubectl get serviceaccount -n airflow. Veja que o ariflow-worker está presente e com o código kubectl get clusterrolebinding -n airflow estará lá também. Pode trocar o get pelo describe e adicione o nome desejado depois do nome anterior para verificar também.  

Precisamos criar um operator (responsável por criar novos recursos no kubernetes) para utilizar o spark application no kubernetes. Para fazer isso precisamos antes utilizar o helm: helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator. Depois de um update no helm com helm repo update. Então poderemos fazer a instalação do spark operator com o código: helm install spark spark-operator/spark-operator -n processing. Para verificar se o spark operator for deployado então é só fazer helm ls -n processing. E depois de uma olhada no kubectl get pods -n processing

No bucket S3 devemos ter 3 pastas, uma para landing (bronze), ou para processar (prata) e outra para entregar (ouro), e uma pasta com os códigos em python com spark (codes). O user guide do spark-operator está em https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md.

Verifique se o segredo já foi criado com o código: kubectl describe secret aws-credentials -n processing. Caso não tenha sido, crie utilizando a chave de acesso do aws, com o seguinte código: kubectl create secret generic aws-credentials --from-literal=aws_access_key_id=meukeyid -from-literal=aws_secret_access_key=meusecretkey -n processing

Desse mesmo user guide pegue o exemplo de spark operator no k8 spark-py-pi.yaml e coloque na subpasta do spark no kubernetes. Vamos realizar algumas customizações nesse arquivo:

-> Linha 5: Troque para processing
-> Linha 6: Adicione 
    volumes:
        - name: ivy
          emptyDir: {}
    sparkConf:
        spark.jars.packages: "org.apache.hadoop-aws:2.7.3.org.apache.spark-avro_2.12:3.0.1"
        spark.driver.extraJavaOptions: "-Divy.cache.dir=/tmp -Divy.home=/tmp"
        spark.kubernetes.allocation.batch.size: "10"
    hadoopConf:
        fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem

-> Linha 19: troque pelo endereço da sua imagem, ou seja, por "docker.io/gabrielnovais/spark-operator:v3.0.0-aws"

-> Linha 21: adicionar endereço do bucket com s3a do código onde está o spark a ser executado em python, logo seria o bucket: "s3a://testekuberneteslogs/codes/spark-operator-processing-job-batch.py"

-> Linha 22: trocar versão para 3.0.0
-> Linha 24: trocar restart policy para o tipo Never, e deletar as linhas relacionadas.

-> Linha 25: dentro de driver escrever o seguinte código

envSecretKeyRefs:
    AWS_ACCESS_KEY_ID:
        name: aws-credentials
        key: aws_access_key_id
    AWS_SECRET_ACCESS_KEY:
        name: aws-credentials
        key: aws_secret_access_key

-> Linha 35: Trocar memory por "4g"
-> Linha 37: Trocar por 3.0.0
-> Linha 38: abaixo dessa linha cole o seguinte código

volumeMounts:
    - name: ivy
      mountPath: /tmp

-> Linha 42 repita o trecho da linha 25
-> Linha 50 e 51: trocar por 3
-> Linha 52: trocar por "4g"
-> Linha 54: Trocar por 3.0.0 e adicionar o trecho da linha 38.

Agora vamos realizar um deploy. Rode o código: kubectl apply -f spark-batch-operator-k8s-v1beta2.yaml -n processing. Isso vai criar o job, criando um container driver no pod e nós executores. Após o job ser completado o driver ficará com o status completed, para ver isso rode: kubexctl get pods -n processing. Verifique que agora podemos rodar o código kubectl get sparkapplication -n processing. Caso queira ver algo a mais do job olhar o comando describe do kubectl. Par coletar os logs do pod, pegue o nome do pod com o comando kubectl get pods -n processing, e depois disso rode o comando kubectl logs (nome do pod) -n processing.

12) Vamos construir a pipeline toda agora

Criar um namespace airflow e deployar o airflow (já feito)
Pegue o endereço do airflow-webserver e acesse a URL para ter a UI do airflow.

Dentro do airflow fazer uma nova conexão além da aws, que é a conexão com o kubernetes. Na conn id escrever um nome diferente com o kubernetes para associar (kubernetes_default por exemplo). No tipo escolher Kubernetes Cluster Connection. Selecionar a caixa de "In cluster configuration". Agora basta criar.

Os dados serão consumidos do bucket da landing zone. Os dados já devem estar lá para que possam ser utilizados. Para cada execução de código spark, precisamos utilizar um arquivo python com o spark e um yaml associado ao job, fora o python da própria DAG. Os códigos sparks que de fato executarão as transformações ficam dentro da pasta pyspark em dags. Essa pasta precisa estar no bucket que contém o folder codes, pois é de lá que esses códigos serão puxado para o cluster kubernetes. Sobre o preenchimento da DAG, em termos de cronograma de execução, pode-se utilizar o crontab guru para consultar como realizar esse cornograma. Em cada arquivo yaml associado ao arquivo pyspark devemos ter uma estrutura similar, mudando apenas o nome base e o nome do arquivo pyspark associado.

As tabelas devem ser as seguintes:
-> dados com colunas de: pessoas, id, pits, categoria do pit, hora e minutos da visita, comprou ou não, preço, quantidade.
-> processar dados acima para distribuir para analitics uma tabela com a seguinte estrutura por dia: qtd de pessoas identificadas, qtd de pits vendidos, visitados, valor arrecadado
-> processar a primeira tabela de forma a gerar uma tabela de recomendação que servirá para a experiência do usuário da seguiinte forma: ids das pessoas, pits vistos, pits recomendados 

Os arquivos em pyspark devem ser:

-> transformar de csv para parquet e salvar na zona de processing
-> gerar as transformações para analytics
-> salvar tabela na zona de consumer
-> trigar o crawler para gerar tabela no athena
-> gerar as transformações para recomendação
-> salvar tabela na zona de consumer
-> trigar o crawler para gerar tabela no athena

Podemos verificar o processo no airflow. Para verificar quais jobs estão rodando no cluster rode o código: kubectl get pods -n airflow --watch. Outro acompanhamento interessante seria olhar o spark application com o código: kubectl get sparkapplication -n airflow. Para acompanhar os logs de execução pegue o pod do driver por exemplo. Caso dê alguma falha por conta de recursos, é possível falhar o processo manualmente e matar os pods que estão pendentes com o kubectl delete + nome do pod.