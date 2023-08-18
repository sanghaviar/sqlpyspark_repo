from util import *
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate()
data = [('James','Sales',3000),
        ('Michael','Sales',4600),
        ('Robert','Sales',4100),
        ('Maria','Finance',3000),
        ('Roman','Finance',3000),
        ('Scott','Finance',3300),
        ('Jen','Finance',3900),
        ('Jeff','Marketing',3000),
        ('Kumar','Marketing',2000)]
schema = ['employee_name','department','salary']
df = spark.createDataFrame(data,schema)
ques3(df)





