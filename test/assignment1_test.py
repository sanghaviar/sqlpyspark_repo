import unittest
from src.Assignment1 import util
from pyspark.sql.types import StructField,StructType,IntegerType,StringType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import col,lit,struct
def test_quest1(df):
    df = df.withColumn("Country",lit("").cast("string"))\
        .withColumn("Department",lit("").cast("string"))\
        .withColumn("Age",lit("").cast("string"))

    df = df.withColumn("salary", col("salary") * 2)

    df = df.withColumn('dob',df['dob'].cast('string'))\
       .withColumn('salary',df['salary'].cast('string'))

    df = df.withColumn('salary_increment',col('salary')*2)

    new_schema = struct(col("name.firstname").alias("firstposition"),
                         col("name.middlename").alias("middleposition"),
                         col("name.lastname").alias("lastposition"))
    df  = df.withColumn("name", new_schema)
    df = df.withColumn('salary',df['salary'].cast('bigint'))
    df = df.select('salary','dob').distinct()
    df = df.drop('Department','age')
    return df


class MyTestCase(unittest.TestCase):
    def test_case1(self):
        schema = StructType([
            StructField("name", StructType([
                StructField("firstname", StringType()),
                StructField("middlename", StringType()),
                StructField("lastname", StringType())
            ])),
            StructField("dob", StringType()),
            StructField("gender", StringType()),
            StructField("salary", IntegerType())
        ])

        data = [
            ({'firstname': 'james', 'middlename': '', 'lastname': 'smith'}, '03011998', 'M', 3000),
            ({'firstname': 'Michael', 'middlename': 'Rose', 'lastname': ''}, '10111998', 'M', 20000),
            ({'firstname': 'Robert', 'middlename': '', 'lastname': 'williams'}, '02012000', 'M', 3000),
            ({'firstname': 'Maria', 'middlename': 'Anne', 'lastname': 'jones'}, '03011998', 'F', 11000),
            ({'firstname': 'Jen', 'middlename': 'Mary', 'lastname': 'brown'}, '04101998', 'F', 100000)
        ]
        df = spark.createDataFrame(data, schema)
        x = util.quest1(df)
        expected_output = test_quest1(df)
        self.assertEqual(x.collect(),expected_output.collect())

    def test_case2(self):
        schema = StructType([
            StructField("name", StructType([
                StructField("firstname", StringType()),
                StructField("middlename", StringType()),
                StructField("lastname", StringType())
            ])),
            StructField("dob", StringType()),
            StructField("gender", StringType()),
            StructField("salary", IntegerType())
        ])

        data = [
            ({'firstname': 'Mona', 'middlename': 'sen', 'lastname': 'gupta'}, '03011998', 'F', 5000),
            ({'firstname': 'John', 'middlename': '', 'lastname': ''}, '10111998', 'M', 90000),
            ({'firstname': 'sara', 'middlename': 'jo', 'lastname': 'williams'}, '02012000', 'F', 9000),
            ({'firstname': 'Mani', 'middlename': 'mohan', 'lastname': 'jones'}, '03011998', 'M', 19000),
            ({'firstname': 'kapil', 'middlename': 'raj', 'lastname': ''}, '04101998', 'M', 800900)
        ]
        df = spark.createDataFrame(data, schema)
        x = util.quest1(df)
        expected_output = test_quest1(df)
        self.assertEqual(x.collect(),expected_output.collect())

    def test_case3(self):
        schema = StructType([
            StructField("name", StructType([
                StructField("firstname", StringType()),
                StructField("middlename", StringType()),
                StructField("lastname", StringType())
            ])),
            StructField("dob", StringType()),
            StructField("gender", StringType()),
            StructField("salary", IntegerType())
        ])

        data = [
            ({'firstname': 'Rama', 'middlename': '', 'lastname': 'kushal'}, '03011998', 'M', 8000),
            ({'firstname': 'Spandhana', 'middlename': 'gupta', 'lastname': ''}, '10111998', 'F', 80000),
            ({'firstname': 'Kushi', 'middlename': '', 'lastname': 'joseph'}, '02012000', 'F', 70000),
            ({'firstname': 'Liz', 'middlename': 'maria', 'lastname': 'joseph'}, '03011998', 'F', 12000),
            ({'firstname': 'jethin', 'middlename': '', 'lastname': 'kumar'}, '04101998', 'M', 150000)
        ]
        df = spark.createDataFrame(data, schema)
        x = util.quest1(df)
        expected_output = test_quest1(df)
        self.assertEqual(x.collect(),expected_output.collect())


if __name__ == '__main__':
    unittest.main()
