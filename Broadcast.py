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

Store = [
    (100,'Store_london'),
    (200,'Store_Paris'),
    (300,'Store_Usa'),
    (400,'Store_India'),
    (500,'Store_SW')
]

column1 = 'Store_id INT,Store_name STRING'

storeDF = spark.createDataFrame(data=Store,schema=column1)
storeDF.show()

from pyspark.sql.functions import broadcast
joinDF = transactionDF.join(broadcast(storeDF),transactionDF['Store_id'] == storeDF['Store_id'])
joinDF.show()