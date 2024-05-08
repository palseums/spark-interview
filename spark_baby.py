from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DateType
data_list = [("palani", "28", "1", "2002"), ("seetha", "28", "5", "81"), ("sree", "12", "12", "6"),
             ("Adwaidan", "23", "5", "81")]
data = [
 ("James,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

schema = StructType([
    StructField("name",StringType(),True),
    StructField("Age",StringType(), True),
    StructField("street_no",StringType(), True),
    StructField("house_no", StringType(), True)
  ])

df = spark.createDataFrame(data=data_list,schema=schema)
df1 = df.filter(df.Age == 28)
df2 = df.groupby("name").count().show()
# print(df.rdd.getNumPartitions())
# df.show()