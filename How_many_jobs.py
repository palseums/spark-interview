# spark.read.format("csv").load(path) - 1 Job ( To find the number of column)
# spark.read.format("csv").option("inferSchema",True).load(path) - 2 Job ( to find the number of column, to find the datatype of each column)
# spark.read.format("csv").schema(schema).load(path) - 0 Job

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DateType,DateConverter
schema = StructType([
    StructField("firstname",StringType(),True),
    StructField("Age",IntegerType(),True),
    StructField("salary", IntegerType(), True),
    StructField("country", StringType(), True)
    
  ])

df_1_job= spark.read.format("csv").load("/FileStore/tables/corrupt/corrupt.csv")
df1_2_job = spark.read.format("csv").option("inferSchema",True).load("/FileStore/tables/corrupt/corrupt.csv")
df_0_job = spark.read.format("csv").schema(schema).load("/FileStore/tables/corrupt/corrupt.csv")