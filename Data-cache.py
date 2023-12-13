from pyspark.sql.functions import *

df = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", False)
    .option("sep", ",")
    .load("/FileStore/tables/orders/part_00000")
)
df1 = (
    df.withColumnRenamed("_c0", "numid")
    .withColumnRenamed("_c1", "order_date")
    .withColumnRenamed("_c2", "order_number")
    .withColumnRenamed("_c3", "order_status")
)
df2 = df1.filter(col('order_status').isin('PENDING_PAYMENT','COMPLETE'))
df2.cache()
df3 = df2.filter(col('order_number') > 1000)
df6 = df2.filter(col('order_number') < 1000)

df5 = df3.groupBy(col("order_status")).count()
df7 = df6.groupBy(col("order_status")).count()

df3.groupBy(col("order_status")).count().write.format("csv").mode("overwrite").save("/FileStore/tables/Result1/")
df6.groupBy(col("order_status")).count().write.format("noop").mode("overwrite").save("/FileStore/tables/Result2/")

# df5.show()
# df7.show()

