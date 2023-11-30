import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DateType,DateConverter
from pyspark.sql.functions import col,struct,when


spark = SparkSession.builder.master("local[1]").appName("StructType").getOrCreate()

spark.sql("select date_format(DAte '1970-1-01', 'LL')").show()