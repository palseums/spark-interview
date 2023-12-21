Transaction = [
    (100,'Cosmetic',150),
    (200,'Apparel',250),
    (300,'Shirt',400),
    (400,'Trouser',500),
    (500,'Socks',20),
    (100,'Belt',250),
    (200,'Cosmetic',400),
    (300,'shoe',25),
    (400,'socks',100),
    (500,'Shorts',70)
]

transactionDF = spark.createDataFrame(data=Transaction,schema=['Store_id','Item','Amount'])
transactionDF.show()
transactionDF.write.format("parquet").option("compression","snappy").save("/FileStore/tables/parquet1/")
transactionDF.write.format("parquet").option("compression","gzip").save("/FileStore/tables/parquet_gzip/")

