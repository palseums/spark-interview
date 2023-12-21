from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DateType
from pyspark.sql.functions import col,explode,split

spark = SparkSession.builder.master("local[1]").appName("StructType").getOrCreate()

data = [
 ("James,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

schema = StructType([
    StructField("name",StringType(),True),
    StructField("languagesAtSchool",ArrayType(StringType()),True),
    StructField("languagesAtWork",ArrayType(StringType()),True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
# Explode function
df_explode = df.select(col("name"),explode(col("languagesAtSchool")).alias("After_exploded"))
df_explode.show()

# Split function
df_split = df.select(col("name"),split(col("name"),",").alias("After_split"))
df_split.printSchema()
df_split.show()





