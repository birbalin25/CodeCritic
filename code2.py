from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr

spark = SparkSession.builder.appName("MultiSourceJoinInefficiency").getOrCreate()

fact_df = spark.range(0, 10000000).withColumn("user_id", (col("id") % 1000000)) \
                                     .withColumn("product_id", (col("id") % 5000)) \
                                     .withColumn("quantity", (col("id") % 5 + 1)) \
                                     .withColumn("amount", col("quantity") * 100 + rand() * 50)

product_df = spark.range(0, 5000).withColumnRenamed("id", "product_id") \
                                 .withColumn("category", expr("CASE WHEN product_id % 2 = 0 THEN 'A' ELSE 'B' END"))

user_df = spark.range(0, 1_000_000).withColumnRenamed("id", "user_id") \
                                   .withColumn("country", expr("CASE WHEN user_id = 42 THEN 'US' ELSE 'IN' END"))

product_df_skewed = product_df.withColumn("product_id", col("product_id").cast("string"))

fact_df_with_product = fact_df.join(product_df_skewed, fact_df["product_id"].cast("string") == product_df_skewed["product_id"], "inner")

fact_df_with_user = fact_df_with_product.join(user_df, on="user_id", how="inner")

agg_df = fact_df_with_user.groupBy("category", "country") \
                          .agg({"amount": "sum", "quantity": "avg"}) \
                          .filter(col("category") == "A")

agg_df.show()