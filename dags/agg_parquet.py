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
            .appname("Customer tratamento Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Pegando os dados tratados
    items = spark.read.parquet("s3a://dl-processing-zone-715036709715/olist/tratados/items/")
    payments = spark.read.parquet("s3a://dl-processing-zone-715036709715/olist/tratados/payments/")
    customers = spark.read.parquet("s3a://dl-processing-zone-715036709715/olist/tratados/custumers/")
    reviews = spark.read.parquet("s3a://dl-processing-zone-715036709715/olist/tratados/reviews/")
    dataset = spark.read.parquet("s3a://dl-processing-zone-715036709715/olist/tratados/dataset/")

    # Agregando dados
    agg1 = items.join(payments , on=['order_id'] , how = 'left')
    agg2 = agg1.join(reviews , on=['order_id'] , how = 'left')
    agg3 = agg2.join(dataset, on=['order_id'] , how = 'left')
    agg = agg3.join(customers , on=['customer_id'] , how = 'left')

    agg.printSchema()


    # Salvando dados agregados
    (
        agg
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-processing-zone-715036709715/olist/agg/")
    )

    (
        agg
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-delivery-zone-715036709715/olist/agg/")
    )

    # Construindo tabela base para a recomendação
    rec = spark.read.parquet("s3a://dl-processing-zone-715036709715/olist/agg/")

    columns = [
        "customer_id",
        "product_id",
        "review_score"
    ]

    rec = rec.select(*columns)

    (
        rec
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-processing-zone-715036709715/olist/rec/")
    )

    print("Agregado com sucesso!!!")

    spark.stop()