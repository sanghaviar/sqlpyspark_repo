
from pyspark.sql.types import StructField,StructType,IntegerType,StringType
from pyspark.sql import SparkSession
# from src.Assignment1 import util
from util import *

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType()),
        StructField("middlename", StringType()),
        StructField("lastname", StringType())
        ])),
    StructField("dob", StringType()),
    StructField("gender",StringType()),
    StructField("salary",IntegerType())
])

data = [
    ({'firstname':'james','middlename':'','lastname':'smith'},'03011998','M',3000),
    ({'firstname': 'Michael', 'middlename': 'Rose', 'lastname': ''},'10111998','M',20000),
    ({'firstname': 'Robert', 'middlename': '', 'lastname': 'williams'},'02012000','M',3000),
    ({'firstname': 'Maria', 'middlename': 'Anne', 'lastname': 'jones'},'03011998','F',11000),
    ({'firstname': 'Jen', 'middlename': 'Mary', 'lastname': 'brown'},'04101998','F',100000)
]
# schema = ['name','dob','gender','salary']
df = spark.createDataFrame(data,schema)

fun1 = add_column(df)
fun2 = changing_values(fun1)
fun3 = change_dataType(fun2)
fun4 = derive_column(fun3)
fun5 = rename_nested_column(fun4)
fun6 = distict_column(fun5)
fun7 = drop_column(fun6)
# util.quest1(df)