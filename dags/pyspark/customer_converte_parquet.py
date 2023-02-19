from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

#set config

conf=(
    SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EvironmentVariableCredentialsProvider")
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3")
)

# apply conifg

sc = SparkContext(conf = conf).getOrCreate()

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appname("Customer converte Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark
        .read
        .format("csv")
        .options(header='true', inferSchema='true', delimiter=",")
        .load("s3a://dl-landing-zone/olist/olist_customers_dataset.csv")
    )

    df.printSchema()

    (
        df
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-processing-zone/olist/customers/")
    )

    print("Escrito com sucesso!!!")

    spark.stop()