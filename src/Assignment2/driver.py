from pyspark.sql import SparkSession
from src.Assignment2 import util
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
util.pivot_asgn(df)
util.unpivoted_df(df)