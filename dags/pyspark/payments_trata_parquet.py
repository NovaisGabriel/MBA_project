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
            .appname("Payments tratamento Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark
        .read
        .format("csv")
        .options(header='true', inferSchema='true', delimiter=",")
        .load("s3a://dl-processing-zone/olist/payments/")
    )

    df.printSchema()

    # Clean data:
    df = df.na.drop()

    # Select important features from data
    columns = [
        "order_id",
        "payment_type",
        "payment_value"
    ]
    
    df = df.select(*columns)

    (
        df
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-processing-zone/olist/tratados/payments/")
    )

    print("Tratado com sucesso!!!")

    spark.stop()