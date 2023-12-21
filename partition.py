# sc.defaultParallelism
# =======================
# The parameter sc.defaultParallelism determines the number of partitions when creating data within spark.
# Default value is 8 so it creates 8 partition by Default

# spark.sql.files.maxPartitionBytes
# ====================================
# When reading data from external system, partition are created based on parameter spark.sql.files.maxPartitionBytes which is default 
# 128 MB

sc.defaultParallelism
spark.conf.get("spark.sql.files.maxPartitionBytes")

from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(10),IntegerType())
df.rdd.getNumPartitions()
df.printSchema()
df.groupBy("value").count()

%fs ls dbfs:/FileStore/tables/orders/

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep",",").load("/FileStore/tables/orders/part_00000")
df.rdd.getNumPartitions()

