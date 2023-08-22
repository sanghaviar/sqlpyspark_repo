from pyspark.sql import SparkSession
from util import *
spark = SparkSession.builder.getOrCreate()


data = [('Banana',1000,'USA'),
        ('Carrots',1500,'INDIA'),
        ('Beans',1600,'Swedan'),
        ('Orange',2000,'UK'),
        ('Orange',2000,'UAE'),
        ('Banana',400,'China'),
        ('Carrots',1200,'China')]
schema = ('Product','Amount','Country')
df =spark.createDataFrame(data,schema)
fun1 = pivot_fun(df)
fun2 = unpivoit_fun(fun1)

