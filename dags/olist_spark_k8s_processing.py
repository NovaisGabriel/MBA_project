import boto3

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

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

def trigger_crawler_landing():
    glue.start_crawler(Name = 'dl_landing_zone_crawler')

def trigger_crawler_processing():
    glue.start_crawler(Name = 'dl_processing_zone_crawler')

def trigger_crawler_delivery():
    glue.start_crawler(Name = 'dl_delivery_zone_crawler')

with DAG(
    'batch_spark_k8s',
    default_args = default_args,
    description='submit spark-pi as SparkApplication on kubernetes',
    schedule_interval = "0 */2 * * *",
    start_date = days_ago(1),
    catchup = False,
    tags = ['spark', 'kubernetes', 'batch']
) as dag:

    # Coverter
    # --------------------------------------------------------
    customer_converte = SparkKubernetesOperator(
        task_id = 'customer_converte',
        namespace = 'airflow',
        application_file = 'customer_converte_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    dataset_converte = SparkKubernetesOperator(
        task_id = 'dataset_converte',
        namespace = 'airflow',
        application_file = 'dataset_converte_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    items_converte = SparkKubernetesOperator(
        task_id = 'items_converte',
        namespace = 'airflow',
        application_file = 'items_converte_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    payments_converte = SparkKubernetesOperator(
        task_id = 'payments_converte',
        namespace = 'airflow',
        application_file = 'payments_converte_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    reviews_converte = SparkKubernetesOperator(
        task_id = 'reviews_converte',
        namespace = 'airflow',
        application_file = 'reviews_converte_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    # Tratar
    # --------------------------------------------------------
    customer_trata = SparkKubernetesOperator(
        task_id = 'customer_trata',
        namespace = 'airflow',
        application_file = 'customer_trata_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    dataset_trata = SparkKubernetesOperator(
        task_id = 'dataset_trata',
        namespace = 'airflow',
        application_file = 'dataset_trata_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    items_trata = SparkKubernetesOperator(
        task_id = 'items_trata',
        namespace = 'airflow',
        application_file = 'items_trata_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    payments_trata = SparkKubernetesOperator(
        task_id = 'payments_trata',
        namespace = 'airflow',
        application_file = 'payments_trata_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    reviews_trata = SparkKubernetesOperator(
        task_id = 'reviews_trata',
        namespace = 'airflow',
        application_file = 'reviews_trata_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    # Agregar
    # --------------------------------------------------------

    agg_parquet = SparkKubernetesOperator(
        task_id = 'agg_parquet',
        namespace = 'airflow',
        application_file = 'agg_parquet.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    # Recomendação
    # --------------------------------------------------------

    recomendacao = SparkKubernetesOperator(
        task_id = 'recomendacao',
        namespace = 'airflow',
        application_file = 'recomendacao.yaml',
        kubernetes_conn_id = "kubernetes_default",
        do_xcom_push = True
    )

    # Crawler triggers
    # --------------------------------------------------------

    trigger_crawler_landing_task = PythonOperator(
        task_id = 'trigger_crawler_landing',
        python_callable = trigger_crawler_landing
    )

    trigger_crawler_processing_task = PythonOperator(
        task_id = 'trigger_crawler_processing',
        python_callable = trigger_crawler_processing
    )

    trigger_crawler_delivery_task = PythonOperator(
        task_id = 'trigger_crawler_delivery',
        python_callable = trigger_crawler_delivery
    )

    # Others
    # --------------------------------------------------------
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

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

# Relações de dependência:
    begin>>[customer_converte, dataset_converte, items_converte, payments_converte, reviews_converte]
    customer_converte >> customer_trata
    dataset_converte >> dataset_trata
    items_converte >> items_trata
    payments_converte>> payments_trata
    reviews_converte >> reviews_trata
    [customer_converte, dataset_converte, items_converte, payments_converte, reviews_converte] >> trigger_crawler_landing_task
    trigger_crawler_landing_task >> agg_parquet >> trigger_crawler_processing_task
    trigger_crawler_processing_task >> recomendacao >> trigger_crawler_delivery_task >> end