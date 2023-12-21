Permissive - Include corrupt record in separate column,For this you have to create a separate column at the end of the Data frame
Drop Malformed - Ignore corrupt records
Fail Fast - Throw exception if corrupt record

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DateType,DateConverter
schema = StructType([
    StructField("firstname",StringType(),True),
    StructField("Age",IntegerType(),True),
    StructField("salary", IntegerType(), True),
    StructField("country", StringType(), True)
    
  ])
#df = spark.read.format("csv").option("mode","DROPMALFORMED").schema(schema).load("/FileStore/tables/corrupt/corrupt.csv")
#df = spark.read.format("csv").option("mode","FAILFAST").schema(schema).load("/FileStore/tables/corrupt/corrupt.csv")
df = spark.read.format("csv").option("mode","PERMISSIVE").schema(schema).load("/FileStore/tables/corrupt/corrupt.csv")
df.show()
