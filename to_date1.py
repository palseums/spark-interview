from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DateType
from pyspark.sql.functions import col,to_date


data = [(1, "john jones",'2012-12-01'),
    (2, "tracey smith",'2012-12-02'),
    (3, "amy sanders",'2012-12-03')]

columns = StructType([
    StructField("Emp_id",IntegerType(),True),
    StructField("firstname",StringType(),True),
    StructField("Date1",StringType(),True)])

spark = SparkSession.builder.master("local[1]").appName("StructType").getOrCreate()

df = spark.createDataFrame(data=data,schema=columns)

df1 = df.withColumn("date2",to_date(col("Date1"),"yyyy-MM-dd"))
df.printSchema()
df1.printSchema()


