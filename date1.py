import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DateType
from pyspark.sql.functions import col,struct,when,udf


spark = SparkSession.builder.master("local[1]").appName("StructType").getOrCreate()

columns = ["Seqno","Name","Date1"]
data = [("1", "john jones","2012-12-01"),
    ("2", "tracey smith","20121202"),
    ("3", "amy sanders","2012-12-03")]


def check_date(check_string):

    try:
        i = datetime.date.fromisoformat(check_string)
        if (i.__class__ == datetime.date):
            return 1
        else:
            return 0
    except Exception as e:
        return 0

# Register the UDF
check_date1 = udf(check_date,IntegerType())

print(check_date('20191204'))

df = spark.createDataFrame(data=data,schema=columns)
# Using the UDF
df1 = df.withColumn("Result1",check_date1(col("Date1")))

df1.printSchema()

df2 = df1.where(col("Result1") == 1)

df2.show()




