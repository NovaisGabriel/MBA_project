from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key_id = os.environ['AWS_SECRET_ACCESS_KEY']

conf = (
    SparkConf()
    .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key_id)
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoodp.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3")
)

sc = SparkContext(conf=conf).getOrCreate()

if __name__ == "main":

    spark = SparkSession\
            .builder\
            .appName("Repartition Job")\
            .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark
        .read
        .format("csv")
        .options(header="true", inferSchema="true", delimiter=";")
        .load("s3://testekuberneteslogs/bronze/titanic.csv")
    )

    df.show()
    df.printSchema()

    (
        df
        .write
        .model("overwrite")
        .format("parquet")
        .save("s3://testekuberneteslogs/prata/")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()