from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import when
from pyspark.ml.feature import StringIndexer

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



    rec = spark.read.parquet("s3a://dl-processing-zone-715036709715/olist/rec/")
    columns = [
        "customer_id",
        "price",
        "review_score"    
    ]

    # Amostra dos dados:
    rec = rec.select(*columns).limit(50000)
    rec = rec.withColumn("review_score",rec.review_score.cast('int')).fillna(0)

    # Codificando as colunas para o modelo:
    indexer = (rec.select("customer_id").distinct().orderBy("customer_id").withColumn("cid", monotonically_increasing_id()))
    rec = rec.join(indexer, ["customer_id"])

    rec = rec.withColumn(
        "priceId",
        when(rec.price <= 10, 1)
        .when(rec.price <= 20, 2)
        .when(rec.price <= 30, 3)
        .when(rec.price <= 40, 4)
        .when(rec.price <= 50, 5)
        .when(rec.price <= 60, 6)
        .when(rec.price <= 70, 7)
        .when(rec.price <= 80, 8)
        .when(rec.price <= 90, 9)
        .when(rec.price <= 100, 10)
        .when(rec.price <= 110, 11)
        .when(rec.price <= 120, 12)
        .when(rec.price <= 130, 13)
        .when(rec.price <= 140, 14)
        .when(rec.price <= 150, 15)
        .otherwise(16))

    columns = [
        "cid",
        "priceId",
        "review_score"
    ]

    rec = rec.select(*columns).distinct().fillna(0)
    rec = (
        rec
        .withColumn("review_score",rec.review_score.cast('int'))
        .withColumn("cid",rec.cid.cast('int'))
        .withColumn("priceId",rec.priceId.cast('int'))
        .fillna(0)
        .orderBy("cid")
    )

    (
        rec
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-delivery-zone-715036709715/olist/rec/")
    )

    # Create test and train set
    (train, test) = rec.randomSplit([0.8, 0.2], seed = 2020)

    model = ALS(userCol="cid", itemCol="priceId", ratingCol="review_score", nonnegative=True, coldStartStrategy="drop").fit(train)
    evaluator=RegressionEvaluator(metricName="rmse",labelCol="review_score",predictionCol="prediction")
    predictions=model.transform(test)
    rmse=evaluator.evaluate(predictions)
    print("New RMSE: ", evaluator.evaluate(model.transform(test)))

    pred = model.transform(rec)

    (
        pred
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-delivery-zone-715036709715/olist/pred/")
    )

    pred.show(10)

    print("Recomendação gerada com sucesso!!!")

    spark.stop()