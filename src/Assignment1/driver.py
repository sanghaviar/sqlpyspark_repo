
from pyspark.sql.types import StructField,StructType,IntegerType,StringType
from pyspark.sql import SparkSession
from src.Assignment1 import util

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

util.quest1(df)