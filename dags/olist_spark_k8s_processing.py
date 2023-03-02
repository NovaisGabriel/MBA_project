import boto3

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")

glue = boto3.client(
    'glue',
     region_name = 'us-east-2',
     aws_access_key_id = aws_access_key_id,
     aws_secret_access_key = aws_secret_access_key
)

default_args = {
    'owner': 'Gabriel',
    'depends_on_past': False,
    'email':['gabrieel.novais@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1
}

def trigger_crwaler_inscricao_func():
    glue.start_crawler(Name = 'enem_anon_crawler')

def trigger_crwaler_final_func():
    glue.start_crawler(Name = 'enem_uf_final_crawler')

with DAG(
    'batch_spark_k8s',
    default_args = default_args,
    description='submit spark-pi as SparkApplication on kubernetes',
    schedule_interval = "0 */2 * * *",
    start_date = days_ago(1),
    catchup = False,
    tags = ['spark', 'kubernetes', 'batch']
) as dag:

    customer_converte = SparkKubernetesOperator(
        task_id = 'customer_converte_parquet',
        namespace = 'airflow',
        application_file = 'customer_converte_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    converte_parquet_monitor = SparkKubernetesSensor(
        application_name = "teste",
        task_id = 'customer_converte_parquet_monitor',
        namespace = 'airflow',
        application_file = "{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name']}}",
        kubernetes_conn_id = "kubernetes_default"
    )

    # converte_parquet = SparkKubernetesOperator(
    #     task_id = 'convert_parquet',
    #     namespace = 'airflow',
    #     application_file = 'enem_converte_parquet.yaml',
    #     kubernetes_conn_id = "kubernetes_default",
    #     do_xcom_push = True
    # )

    # converte_parquet_monitor = SparkKubernetesSensor(
    #     task_id = 'convert_parquet_monitor',
    #     namespace = 'airflow',
    #     application_file = "{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name']}}",
    #     kubernetes_conn_id = "kubernetes_default"
    # )

    # anonimiza_inscricao = SparkKubernetesOperator(
    #     task_id = 'anonimiza_inscricao',
    #     namespace = 'airflow',
    #     application_file = 'enem_anonimiza_inscricao.yaml',
    #     kubernetes_conn_id = "kubernetes_default",
    #     do_xcom_push = True
    # )

    # anonimiza_inscricao_monitor = SparkKubernetesSensor(
    #     task_id = 'anonimiza_inscricao_monitor',
    #     namespace = 'airflow',
    #     application_file = "{{ task_instance.xcom_pull(task_ids='anonimiza_inscricao')['metadata']['name']}}",
    #     kubernetes_conn_id = "kubernetes_default"
    # )

    # trigger_crwaler_inscricao = PythonOperator(
    #     task_id = 'trigger_crwaler_inscricao',
    #     python_callable = trigger_crwaler_inscricao_func
    # )

    # agrega_idade = SparkKubernetesOperator(
    #     task_id = 'agrega_idade',
    #     namespace = 'airflow',
    #     application_file = 'enem_agrega_idade.yaml',
    #     kubernetes_conn_id = "kubernetes_default",
    #     do_xcom_push = True
    # )

    # agrega_idade_monitor = SparkKubernetesSensor(
    #     task_id = 'agrega_idade_monitor',
    #     namespace = 'airflow',
    #     application_file = "{{ task_instance.xcom_pull(task_ids='agrega_idade')['metadata']['name']}}",
    #     kubernetes_conn_id = "kubernetes_default"
    # )
    
    # trigger_crwaler_final = PythonOperator(
    #     task_id = 'trigger_crwaler_final',
    #     python_callable = trigger_crwaler_final_func
    # )

    customer_converte
    # converte_parquet >> converte_parquet_monitor >> anonimiza_inscricao >> anonimiza_inscricao_monitor
    # anonimiza_inscricao_monitor >> trigger_crwaler_inscricao
    # converte_parquet_monitor >> agrega_idade >> agrega_idade_monitor >> trigger_crwaler_final